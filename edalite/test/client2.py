#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Edalite Caller Implementations for Deferred Tasks
==================================================

This module contains two caller classes that can be used to enqueue deferred tasks
and retrieve their results.

Workflow Overview
-----------------
Both callers implement the following deferred task workflow:
  1. A unique task_id is generated.
  2. An initial task state is stored in Redis with status "QUEUED".
  3. A JSON payload containing the task_id and the task data is published to a given
     NATS subject.
  4. On the worker side (not shown here), when the task is processed the task status
     is updated from QUEUED → PROCESSING → COMPLETED (or FAILURE on error). In addition,
     the worker is expected to publish a notification event to a Redis Pub/Sub channel
     (see below) when the task reaches a terminal state.
  5. The caller retrieves the result:
       - The synchronous caller uses polling.
       - The asynchronous caller uses an event‐driven (Pub/Sub) approach.

For the asynchronous event‐based approach the channel name is formatted as:

    DEFERRED_TASKS_UPDATE_{subject}:{task_id}

where dots in the subject are replaced by underscores. The worker must publish to
this channel after updating the task result in Redis.

Classes
-------
EdaliteCaller
    Synchronous client using polling.
AsyncEdaliteCaller
    Asynchronous client using event‐based notifications via Redis Pub/Sub.

Examples
--------
Synchronous Example:

    caller = EdaliteCaller(
        nats_url="nats://localhost:4222",
        redis_url="redis://localhost:6379/0",
        debug=True
    ).connect()
    
    task_id = caller.delay("service.deferred", "Hello Deferred!")
    print(f"Task enqueued with task_id: {task_id}")
    
    # This will poll until the task reaches COMPLETED or FAILURE.
    result = caller.get("service.deferred", task_id, poll_interval=0.5, timeout=30.0)
    print("Task result:", result)
    
    caller.close()

Asynchronous Example:

    import asyncio
    from datetime import timedelta
    from async_edalite_caller import AsyncEdaliteCaller

    async def test_async_client():
        caller = await AsyncEdaliteCaller.connect(
            nats_url="nats://localhost:4222",
            redis_url="redis://localhost:6379/1",
            debug=True
        )
        task_id = await caller.delay("service.deferred", "Hello Async Deferred!",
                                     ttl=timedelta(seconds=60))
        print(f"[Async Caller] Task enqueued with task_id: {task_id}")
        result = await caller.get("service.deferred", task_id, timeout=30.0)
        print(f"[Async Caller] Task result: {result}")
        await caller.close()

    if __name__ == '__main__':
        asyncio.run(test_async_client())
"""

import asyncio
import uuid
import json
import threading
import time
from datetime import timedelta
from typing import Any, Union, List

from nats.aio.client import Client as NATS
from redis.asyncio import Redis as RedisAsync
from redis import Redis


##############################################################################
# Synchronous Caller Implementation (Deferred Tasks Only)
##############################################################################
class EdaliteCaller:
    """
    Synchronous client for enqueuing deferred tasks and retrieving their results.

    This client generates a unique task_id for each deferred task, immediately stores an
    initial status (QUEUED) in Redis, and publishes the task to the NATS subject. It also
    provides a method to wait for the task result until it reaches a terminal state (COMPLETED
    or FAILURE) using polling.
    """

    def __init__(
        self,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
    ):
        """
        Initialize the EdaliteCaller with connection parameters for NATS and Redis.

        Parameters
        ----------
        nats_url : Union[str, List[str]], optional
            The URL or list of URLs for the NATS server(s). Default is "nats://localhost:4222".
        redis_url : str, optional
            The URL for the Redis server. Default is "redis://localhost:6379/0".
        debug : bool, optional
            If True, enables detailed debug output. Default is False.
        """
        self.nats_url = nats_url
        self.redis_url = redis_url
        self.debug = debug

        self._nc = None  # Asynchronous NATS client.
        self._redis = None  # Synchronous Redis client.
        self.loop = None  # Background asyncio event loop.

    def connect(self) -> "EdaliteCaller":
        """
        Establish connections to the NATS server and Redis, and start the background event loop.

        Returns
        -------
        EdaliteCaller
            The caller instance with established connections.
        """
        self.loop = asyncio.new_event_loop()
        loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        loop_thread.start()

        future = asyncio.run_coroutine_threadsafe(self._init_nats(), self.loop)
        try:
            future.result()
        except Exception as e:
            if self.debug:
                print(f"Error during connection: {e}")
        if self.debug:
            print("EdaliteCaller connected.")
        return self

    def _run_loop(self):
        """
        Run the asyncio event loop in a background thread.
        """
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _init_nats(self):
        """
        Asynchronously initialize the NATS client and the synchronous Redis client.

        Connects to the NATS server and instantiates the Redis client.
        """
        self._nc = NATS()
        servers = self.nats_url if isinstance(self.nats_url, list) else [self.nats_url]
        await self._nc.connect(
            servers=servers,
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
            ping_interval=20,
            max_outstanding_pings=5,
        )
        self._redis = Redis.from_url(self.redis_url, decode_responses=True)

    def delay(self, subject: str, data: Any, ttl: timedelta = timedelta(days=1)) -> str:
        """
        Enqueue a deferred task.

        This method generates a unique task_id, stores an initial state (QUEUED) in Redis,
        and publishes the task to the specified NATS subject. The published message is a JSON
        document that includes the task_id and the task data.

        Parameters
        ----------
        subject : str
            The NATS subject to which the task is published.
        data : Any
            The task data.
        ttl : timedelta, optional
            The time-to-live for the Redis key storing the task result. Default is 1 day.

        Returns
        -------
        str
            The unique task_id assigned to the deferred task.

        Raises
        ------
        RuntimeError
            If storing the initial task state in Redis or publishing to NATS fails.
        """
        task_id = str(uuid.uuid4())
        redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
        # Store the initial state as QUEUED.
        doc = {"task_id": task_id, "status": "QUEUED", "result": None}
        try:
            self._redis.set(redis_key, json.dumps(doc), ex=int(ttl.total_seconds()))
        except Exception as e:
            raise RuntimeError(f"Error storing initial task state: {e}")

        # Create a payload containing the task_id and the actual task data.
        payload = {"task_id": task_id, "data": data}
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._nc.publish(subject, json.dumps(payload).encode()), self.loop
            )
            future.result()
        except Exception as e:
            raise RuntimeError(f"Error publishing task to NATS: {e}")

        if self.debug:
            print(f"[Caller] Task {task_id} enqueued with status QUEUED.")
        return task_id

    def get(
        self,
        subject: str,
        task_id: str,
        poll_interval: float = 0.5,
        timeout: float = 30.0,
    ) -> dict:
        """
        Wait for the result of a deferred task until it reaches a terminal state (COMPLETED or FAILURE).

        This method polls the Redis key associated with the task at regular intervals until the
        status becomes either COMPLETED or FAILURE. If the result is not available within the specified
        timeout, a TimeoutError is raised.

        Parameters
        ----------
        subject : str
            The subject associated with the deferred task.
        task_id : str
            The unique identifier of the deferred task.
        poll_interval : float, optional
            The interval (in seconds) between polling attempts. Default is 0.5 seconds.
        timeout : float, optional
            The maximum time (in seconds) to wait for the task result. Default is 30.0 seconds.

        Returns
        -------
        dict
            A dictionary containing the task_id, status, and result.

        Raises
        ------
        TimeoutError
            If the task result is not available within the specified timeout.
        """
        start_time = time.time()
        redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
        while True:
            value = self._redis.get(redis_key)
            if value is not None:
                data = json.loads(value)
                if data.get("status") in ["COMPLETED", "FAILURE"]:
                    return data
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for task {task_id} to complete.")
            time.sleep(poll_interval)

    def close(self):
        """
        Close the NATS connection and stop the background asyncio event loop.
        """
        asyncio.run_coroutine_threadsafe(self._nc.close(), self.loop).result()
        self.loop.call_soon_threadsafe(self.loop.stop)


##############################################################################
# Asynchronous Caller Implementation (Deferred Tasks Only, Event-Based)
##############################################################################
class AsyncEdaliteCaller:
    """
    Asynchronous client for enqueuing deferred tasks and retrieving their results.

    This client generates a unique task_id for each deferred task, immediately stores an
    initial status (QUEUED) in Redis, and publishes the task to a NATS subject using an
    asynchronous NATS client. The caller can then await the task result until it reaches a
    terminal state (COMPLETED or FAILURE) using an event-based mechanism via Redis Pub/Sub.

    Note:
        For the event-based get() method to work, the worker (or another component) must
        publish a notification message to a Redis channel once the task's status is updated.
        The channel name is formatted as:
            DEFERRED_TASKS_UPDATE_{subject}:{task_id}
        where dots in the subject are replaced by underscores.
    """

    def __init__(
        self,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
    ):
        """
        Initialize the AsyncEdaliteCaller with connection parameters.

        Parameters
        ----------
        nats_url : Union[str, List[str]], optional
            The URL or list of URLs for the NATS server(s). Default is "nats://localhost:4222".
        redis_url : str, optional
            The URL for the Redis server. Default is "redis://localhost:6379/0".
        debug : bool, optional
            If True, enables detailed debug output. Default is False.
        """
        self.nats_url = nats_url
        self.redis_url = redis_url
        self.debug = debug

        self._nc = None      # Asynchronous NATS client.
        self._redis = None   # Asynchronous Redis client.

    @classmethod
    async def connect(
        cls,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
    ) -> "AsyncEdaliteCaller":
        """
        Asynchronously create an instance of AsyncEdaliteCaller and establish connections.

        This method connects to the NATS server and instantiates the asynchronous Redis client.

        Parameters
        ----------
        nats_url : Union[str, List[str]], optional
            The URL or list of URLs for the NATS server(s). Default is "nats://localhost:4222".
        redis_url : str, optional
            The URL for the Redis server. Default is "redis://localhost:6379/0".
        debug : bool, optional
            If True, enables detailed debug output. Default is False.

        Returns
        -------
        AsyncEdaliteCaller
            An instance of AsyncEdaliteCaller with established connections.
        """
        instance = cls(nats_url, redis_url, debug)
        instance._nc = NATS()
        servers = instance.nats_url if isinstance(instance.nats_url, list) else [instance.nats_url]
        await instance._nc.connect(
            servers=servers,
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
            ping_interval=20,
            max_outstanding_pings=5,
        )
        instance._redis = RedisAsync.from_url(instance.redis_url, decode_responses=True)
        if debug:
            print("[AsyncEdaliteCaller] Connected to NATS and Redis.")
        return instance

    async def delay(self, subject: str, data: Any, ttl: timedelta = timedelta(days=1)) -> str:
        """
        Enqueue a deferred task.

        This method generates a unique task_id, stores an initial state (QUEUED) in Redis,
        and publishes a JSON payload containing the task_id and the task data to the specified
        NATS subject.

        Parameters
        ----------
        subject : str
            The NATS subject to which the task is published.
        data : Any
            The task data.
        ttl : timedelta, optional
            The time-to-live for the Redis key storing the task result. Default is 1 day.

        Returns
        -------
        str
            The unique task_id assigned to the deferred task.

        Raises
        ------
        RuntimeError
            If storing the initial task state in Redis or publishing to NATS fails.
        """
        task_id = str(uuid.uuid4())
        redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
        # Prepare initial state document with status QUEUED.
        doc = {"task_id": task_id, "status": "QUEUED", "result": None}
        try:
            await self._redis.set(redis_key, json.dumps(doc), ex=int(ttl.total_seconds()))
        except Exception as e:
            raise RuntimeError(f"Error storing initial task state: {e}")

        # Create payload with task_id and user-provided data.
        payload = {"task_id": task_id, "data": data}
        try:
            await self._nc.publish(subject, json.dumps(payload).encode())
        except Exception as e:
            raise RuntimeError(f"Error publishing task to NATS: {e}")

        if self.debug:
            print(f"[AsyncEdaliteCaller] Task {task_id} enqueued with status QUEUED.")
        return task_id

    async def get(
        self,
        subject: str,
        task_id: str,
        timeout: float = 30.0,
    ) -> dict:
        """
        Wait for the result of a deferred task using an event-based approach via Redis Pub/Sub.

        This method subscribes to a Redis channel that is expected to receive a notification
        when the task's status is updated to a terminal state (COMPLETED or FAILURE). The channel
        name is formatted as:
            DEFERRED_TASKS_UPDATE_{subject}:{task_id}
        (with dots in the subject replaced by underscores). After receiving an event, the task
        result is read from Redis.

        Parameters
        ----------
        subject : str
            The subject associated with the deferred task.
        task_id : str
            The unique identifier of the deferred task.
        timeout : float, optional
            The maximum time (in seconds) to wait for the task result. Default is 30.0 seconds.

        Returns
        -------
        dict
            A dictionary containing the task_id, status, and result.

        Raises
        ------
        TimeoutError
            If the task result is not received within the specified timeout.
        """
        redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
        channel = f"DEFERRED_TASKS_UPDATE_{subject.replace('.', '_')}:{task_id}"

        # First, check if the task is already in a terminal state.
        value = await self._redis.get(redis_key)
        if value is not None:
            data = json.loads(value)
            if data.get("status") in ["COMPLETED", "FAILURE"]:
                return data

        # Subscribe to the notification channel.
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(channel)
        try:
            # Wait for a message on the channel with a timeout.
            # The worker (or another component) must publish to this channel after updating the task status.
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)
            if msg is None:
                raise TimeoutError(f"Timeout waiting for task {task_id} to complete.")
            # After receiving an event, retrieve the task result.
            value = await self._redis.get(redis_key)
            if value is None:
                raise RuntimeError(f"Task result for {task_id} not found after notification.")
            data = json.loads(value)
            if data.get("status") in ["COMPLETED", "FAILURE"]:
                return data
            else:
                raise RuntimeError(f"Received update event, but task {task_id} is not in a terminal state.")
        finally:
            await pubsub.unsubscribe(channel)

    async def close(self):
        """
        Asynchronously close the NATS and Redis connections.

        This method gracefully shuts down the asynchronous NATS client and Redis client.
        """
        await self._nc.close()
        await self._redis.close()
        if self.debug:
            print("[AsyncEdaliteCaller] Connections closed.")
