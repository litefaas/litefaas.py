#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Edalite Deferred Task Implementation with TTL and Request/Reply
================================================================

This module provides a deferred task system similar to Celery’s delay functionality.
A client “delay” request now accepts two TTLs:
  - queue_ttl: the expiration period for the queued request. The worker will check the
               timestamp and, if expired, update the task’s state to EXPIRED and skip processing.
  - task_ttl: the TTL for storing the task result in Redis.

All requests include an "action" field in the payload:
  - For delay requests, "action": "delay" (Redis storage is used).
  - For get requests, "action": "get" (no Redis storage is performed; the worker simply looks up and replies).

Both synchronous and asynchronous caller classes use the NATS request/reply pattern.

Classes
-------
EdaliteWorker
    Synchronous worker that processes deferred tasks.
EdaliteCaller
    Synchronous client that sends delay and get requests via NATS.
AsyncEdaliteCaller
    Asynchronous client that sends delay and get requests via NATS.

Examples
--------
Synchronous usage:

    from datetime import timedelta
    import time, threading
    from edalite_module import EdaliteWorker, EdaliteCaller

    # Define a deferred task.
    worker = EdaliteWorker(
        nats_url="nats://localhost:4222",
        redis_url="redis://localhost:6379/0",
        debug=True,
        max_thread=2,    # Use 2 threads.
        max_process=1    # Single process.
    )

    @worker.delayed_task("service.deferred", ttl=timedelta(seconds=60))
    def deferred_task(data):
        print(f"Worker processing deferred task with data: {data}")
        import time
        time.sleep(2)
        return f"Processed deferred data: {data}"

    # Start the worker in a background thread.
    threading.Thread(target=worker.start, daemon=True).start()
    time.sleep(1)

    # Create a synchronous caller.
    caller = EdaliteCaller(
        nats_url="nats://localhost:4222",
        redis_url="redis://localhost:6379/0",
        debug=True
    ).connect()

    # Send a delay request with queue_ttl and task_ttl.
    task_id = caller.delay(
        "service.deferred",
        "Hello Deferred!",
        queue_ttl=timedelta(seconds=5),   # The request is valid for 5 seconds.
        task_ttl=timedelta(seconds=60)    # The task result is stored for 60 seconds.
    )
    print(f"[Caller] Delay ACK received; task_id: {task_id}")

    # Later, use a get request (via NATS request/reply) to retrieve the result.
    result = caller.get("service.deferred", task_id, timeout=30)
    print(f"[Caller] Get response: {result}")

    caller.close()


Asynchronous usage:

    import asyncio
    from datetime import timedelta
    from edalite_module import AsyncEdaliteCaller

    async def test_async_client():
        caller = await AsyncEdaliteCaller.connect(
            nats_url="nats://localhost:4222",
            redis_url="redis://localhost:6379/1",
            debug=True
        )
        task_id = await caller.delay(
            "service.deferred",
            "Hello Async Deferred!",
            queue_ttl=timedelta(seconds=5),
            task_ttl=timedelta(seconds=60)
        )
        print(f"[Async Caller] Delay ACK received; task_id: {task_id}")
        result = await caller.get("service.deferred", task_id, timeout=30)
        print(f"[Async Caller] Get response: {result}")
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
from typing import Callable, Any, Union, List
import os  # For process ID
import concurrent.futures
import multiprocessing

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NATSMessage
from redis import Redis
from redis.asyncio import Redis as RedisAsync


##############################################################################
# Synchronous Worker Implementation (Deferred Tasks Only)
##############################################################################
class EdaliteWorker:
    """
    Synchronous worker for processing deferred tasks via a NATS messaging system.

    The worker registers deferred task functions. When a message is received on a registered
    subject, the payload must include an "action" field:
      - "delay": the worker stores the task in Redis (using task_ttl) and immediately replies
                 with an ACK (including task_id). It then processes the task asynchronously.
                 Before processing, it checks if the current time exceeds (timestamp + queue_ttl).
                 If so, it updates the status to EXPIRED and skips execution.
      - "get": the worker looks up the task result in Redis and replies with it.

    Task status flows:
         QUEUED   → (immediately stored) → PROCESSING → COMPLETED (or FAILURE)
         If the queued delay expires (queue_ttl exceeded), the state becomes EXPIRED.
    """

    def __init__(
        self,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
        max_thread: int = 1,
        max_process: int = 1,
    ):
        """
        Initialize the EdaliteWorker.

        Parameters
        ----------
        nats_url : Union[str, List[str]], optional
            URL(s) for the NATS server.
        redis_url : str, optional
            URL for the Redis server.
        debug : bool, optional
            If True, enables debug output.
        max_thread : int, optional
            Number of threads to process tasks.
        max_process : int, optional
            Number of processes to run.
        """
        self.nats_url = nats_url
        self.redis_url = redis_url
        self.debug = debug
        self.max_thread = max_thread
        self.max_process = max_process

        # Dictionary for deferred task functions.
        # Key: subject; Value: list of tuples (function, queue_group, ttl)
        self.deferred_functions = {}

        self._nc = None            # NATS client instance.
        self._redis = None         # Synchronous Redis client.
        self.loop = None           # asyncio event loop.
        self.task_executor = None  # Executor for running tasks.

        # Use 'spawn' for multiprocessing.
        self._mp_context = multiprocessing.get_context("spawn")

    def delayed_task(
        self, subject: str, queue_group: str = None, ttl: timedelta = timedelta(days=1)
    ) -> Callable:
        """
        Decorator for registering a deferred task.

        When a message is received on the subject, the worker expects a JSON payload
        containing "action", "task_id", "data", "queue_ttl", "task_ttl", and "timestamp".
        Based on the action ("delay" or "get"), it processes accordingly.

        Parameters
        ----------
        subject : str
            The NATS subject.
        queue_group : str, optional
            Queue group name for load balancing.
        ttl : timedelta, optional
            Default TTL for the Redis key (task_ttl).

        Returns
        -------
        Callable
            The decorator that registers the task function.
        """
        def decorator(task: Callable):
            if subject not in self.deferred_functions:
                self.deferred_functions[subject] = []
            for existing_task, existing_queue, _ in self.deferred_functions.get(subject, []):
                if existing_queue == queue_group:
                    raise ValueError(
                        f"Queue group '{queue_group}' is already registered for subject '{subject}'"
                    )
            self.deferred_functions[subject].append((task, queue_group, ttl))
            return task
        return decorator

    def start(self):
        """
        Start the worker.

        If max_process > 1, spawns multiple processes; otherwise runs in a single process.
        Sets up the asyncio loop, initializes NATS and Redis, and subscribes to the deferred task subjects.
        """
        if self.max_process > 1:
            print("---------------------------------------")
            print(f"Starting {self.max_process} processes... (PID: {os.getpid()})")
            print("---------------------------------------\n")
            processes = []
            for _ in range(self.max_process):
                p = self._mp_context.Process(target=self._run_in_new_process)
                p.start()
                processes.append(p)
            for p in processes:
                p.join()
        else:
            self._start_single()

    def _run_in_new_process(self):
        """Run the worker in a new process."""
        self._start_single()

    def _start_single(self):
        """
        Start the worker in a single process.

        Sets up the event loop, NATS and Redis connections, the task executor,
        and subscribes to all registered deferred task subjects.
        """
        self.loop = asyncio.new_event_loop()
        loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        loop_thread.start()

        # Configure the task executor.
        if self.max_thread > 1:
            self.task_executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_thread)
        else:
            self.task_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        future = asyncio.run_coroutine_threadsafe(self._init_nats(), self.loop)
        try:
            future.result()
        except Exception as e:
            if self.debug:
                print(f"Error during NATS initialization: {e}")
            return

        if self.debug:
            mode = "multi-process" if self.max_process > 1 else "single-process"
            print(f"EdaliteWorker running in {mode} mode. (PID: {os.getpid()})")
        print("================================================")
        try:
            # Keep the worker running.
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            if self.debug:
                print(f"Worker shutting down... (PID: {os.getpid()})")
            asyncio.run_coroutine_threadsafe(self._nc.close(), self.loop).result()
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.task_executor.shutdown(wait=False)

    def _run_loop(self):
        """Run the asyncio event loop in a background thread."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _init_nats(self):
        """
        Initialize NATS and Redis connections, and subscribe to all deferred task subjects.
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
        if self.debug:
            print(f"Connected to NATS server. (PID: {os.getpid()})")
        self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        if self.debug:
            print(f"Connected to Redis at {self.redis_url}. (PID: {os.getpid()})")
        # Subscribe to each subject registered for deferred tasks.
        for subject, handlers in self.deferred_functions.items():
            for task, queue_group, default_ttl in handlers:
                async def callback(msg: NATSMessage, f=task, used_ttl=default_ttl):
                    self._handle_deferred_message(msg, f, used_ttl)
                if queue_group:
                    await self._nc.subscribe(subject, cb=callback, queue=queue_group)
                else:
                    await self._nc.subscribe(subject, cb=callback)
        if self.debug:
            print("Subscribed to deferred task subjects:")
            print("  Deferred:", list(self.deferred_functions.keys()))
            print(f"(PID: {os.getpid()})")

    @staticmethod
    def _execute_task(task: Callable, data: Any):
        """
        Execute the given task with provided data.
        """
        return task(data)

    def _update_deferred_status(
        self, subject: str, task_id: str, status: str, result: Any, ttl: timedelta
    ):
        """
        Update the Redis key for a task with the given status and result.
        """
        redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
        doc = {"task_id": task_id, "status": status, "result": result}
        try:
            self._redis.set(redis_key, json.dumps(doc), ex=int(ttl.total_seconds()))
        except Exception as e:
            if self.debug:
                print(f"[Redis] Error updating {redis_key}: {e}")

    def _handle_deferred_message(self, msg: NATSMessage, task: Callable, used_ttl: timedelta):
        """
        Process an incoming deferred task message.

        Distinguishes between "delay" and "get" actions based on the payload.
        For "delay":
            - Stores initial state in Redis (QUEUED) with task_ttl.
            - Immediately replies with an ACK (including task_id).
            - Submits task processing to the executor (after checking queue_ttl expiration).
        For "get":
            - Reads the task result from Redis and replies.
        """
        try:
            payload = json.loads(msg.data.decode())
            action = payload.get("action", "delay")
            subject = msg.subject
            if action == "delay":
                task_id = payload["task_id"]
                queue_ttl = payload.get("queue_ttl")         # in seconds
                task_ttl = payload.get("task_ttl")            # in seconds
                timestamp = payload.get("timestamp")          # epoch seconds
                redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
                # Store initial state as QUEUED.
                self._redis.set(redis_key, json.dumps({"task_id": task_id, "status": "QUEUED", "result": None}), ex=int(task_ttl))
                # Immediately reply with ACK.
                asyncio.run_coroutine_threadsafe(msg.respond(f"ACK:{task_id}".encode()), self.loop)
                # Process the task asynchronously.
                def process_task():
                    now = time.time()
                    if now > timestamp + queue_ttl:
                        # Queue expired.
                        self._update_deferred_status(subject, task_id, "EXPIRED", None, timedelta(seconds=task_ttl))
                        if self.debug:
                            print(f"[Deferred] Task {task_id} expired (queue_ttl exceeded). (PID: {os.getpid()})")
                        return
                    self._update_deferred_status(subject, task_id, "PROCESSING", None, timedelta(seconds=task_ttl))
                    if self.debug:
                        print(f"[Deferred] Processing task {task_id} with data: {payload.get('data')} (PID: {os.getpid()})")
                    try:
                        result = self._execute_task(task, payload.get("data"))
                        self._update_deferred_status(subject, task_id, "COMPLETED", result, timedelta(seconds=task_ttl))
                        if self.debug:
                            print(f"[Deferred] Task {task_id} completed with result: {result} (PID: {os.getpid()})")
                    except Exception as e:
                        self._update_deferred_status(subject, task_id, "FAILURE", str(e), timedelta(seconds=task_ttl))
                        if self.debug:
                            print(f"[Deferred] Task {task_id} failed: {e} (PID: {os.getpid()})")
                self.task_executor.submit(process_task)
            elif action == "get":
                # Process a get request.
                task_id = payload["task_id"]
                redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
                value = self._redis.get(redis_key)
                if value is None:
                    asyncio.run_coroutine_threadsafe(msg.respond(f"NOT_FOUND:{task_id}".encode()), self.loop)
                else:
                    asyncio.run_coroutine_threadsafe(msg.respond(value.encode()), self.loop)
            else:
                asyncio.run_coroutine_threadsafe(msg.respond("ERROR: Unknown action".encode()), self.loop)
        except Exception as e:
            if self.debug:
                print(f"[Deferred] Exception in handling message: {e} (PID: {os.getpid()})")
            asyncio.run_coroutine_threadsafe(msg.respond(f"ERROR:{str(e)}".encode()), self.loop)


##############################################################################
# Synchronous Caller Implementation (Deferred Tasks Only, Request/Reply)
##############################################################################
class EdaliteCaller:
    """
    Synchronous client for enqueuing deferred tasks and retrieving their results
    using the NATS request/reply pattern.

    The delay() method sends a request with action "delay" (which uses Redis for storage),
    and the get() method sends a request with action "get" (which simply looks up the result).
    """

    def __init__(
        self,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
    ):
        """
        Initialize the EdaliteCaller.

        Parameters
        ----------
        nats_url : Union[str, List[str]], optional
            URL(s) for the NATS server.
        redis_url : str, optional
            URL for the Redis server.
        debug : bool, optional
            If True, enables debug output.
        """
        self.nats_url = nats_url
        self.redis_url = redis_url
        self.debug = debug

        self._nc = None  # Asynchronous NATS client.
        self._redis = None  # Synchronous Redis client.
        self.loop = None  # Background asyncio event loop.

    def connect(self) -> "EdaliteCaller":
        """
        Establish connections to NATS and Redis, and start the background event loop.

        Returns
        -------
        EdaliteCaller
            The connected caller instance.
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
        """Run the asyncio event loop in a background thread."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _init_nats(self):
        """
        Asynchronously initialize the NATS client and the synchronous Redis client.
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

    def delay(
        self, subject: str, data: Any, queue_ttl: timedelta, task_ttl: timedelta
    ) -> str:
        """
        Enqueue a deferred task via NATS request/reply.

        Parameters
        ----------
        subject : str
            The NATS subject.
        data : Any
            The task data.
        queue_ttl : timedelta
            The expiration period for the queued request.
        task_ttl : timedelta
            The TTL for storing the task result in Redis.

        Returns
        -------
        str
            The generated task_id.
        """
        task_id = str(uuid.uuid4())
        payload = {
            "action": "delay",
            "task_id": task_id,
            "data": data,
            "queue_ttl": int(queue_ttl.total_seconds()),
            "task_ttl": int(task_ttl.total_seconds()),
            "timestamp": time.time()
        }
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._nc.request(subject, json.dumps(payload).encode(), timeout=30),
                self.loop
            )
            response = future.result(30)
            resp_text = response.data.decode()
            if self.debug:
                print(f"[Caller] Delay response: {resp_text}")
            return task_id
        except Exception as e:
            raise RuntimeError(f"Delay request failed: {e}")

    def get(self, subject: str, task_id: str, timeout: float = 30.0) -> dict:
        """
        Retrieve the result of a deferred task using NATS request/reply.

        Parameters
        ----------
        subject : str
            The NATS subject.
        task_id : str
            The unique task identifier.
        timeout : float, optional
            Timeout in seconds for the request.

        Returns
        -------
        dict
            The task result (as a dict if stored in Redis) or a message.
        """
        payload = {
            "action": "get",
            "task_id": task_id
        }
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._nc.request(subject, json.dumps(payload).encode(), timeout=timeout),
                self.loop
            )
            response = future.result(timeout)
            resp_text = response.data.decode()
            if self.debug:
                print(f"[Caller] Get response: {resp_text}")
            try:
                data = json.loads(resp_text)
                return data
            except Exception:
                return {"message": resp_text}
        except Exception as e:
            raise RuntimeError(f"Get request failed: {e}")

    def close(self):
        """
        Close the NATS connection and stop the background asyncio event loop.
        """
        asyncio.run_coroutine_threadsafe(self._nc.close(), self.loop).result()
        self.loop.call_soon_threadsafe(self.loop.stop)


##############################################################################
# Asynchronous Caller Implementation (Deferred Tasks Only, Request/Reply)
##############################################################################
class AsyncEdaliteCaller:
    """
    Asynchronous client for enqueuing deferred tasks and retrieving their results
    using the NATS request/reply pattern.

    The delay() method sends a delay request (with Redis storage) and returns a task_id.
    The get() method sends a get request and awaits the worker’s reply.
    """

    def __init__(
        self,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
    ):
        """
        Initialize the AsyncEdaliteCaller.

        Parameters
        ----------
        nats_url : Union[str, List[str]], optional
            URL(s) for the NATS server.
        redis_url : str, optional
            URL for the Redis server.
        debug : bool, optional
            If True, enables debug output.
        """
        self.nats_url = nats_url
        self.redis_url = redis_url
        self.debug = debug

        self._nc = None       # Asynchronous NATS client.
        self._redis = None    # Asynchronous Redis client.

    @classmethod
    async def connect(
        cls,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
    ) -> "AsyncEdaliteCaller":
        """
        Asynchronously create and connect an AsyncEdaliteCaller instance.

        Returns
        -------
        AsyncEdaliteCaller
            The connected instance.
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

    async def delay(self, subject: str, data: Any, queue_ttl: timedelta, task_ttl: timedelta) -> str:
        """
        Enqueue a deferred task via NATS request/reply.

        Parameters
        ----------
        subject : str
            The NATS subject.
        data : Any
            The task data.
        queue_ttl : timedelta
            Expiration period for the queue request.
        task_ttl : timedelta
            TTL for storing the task result in Redis.

        Returns
        -------
        str
            The generated task_id.
        """
        task_id = str(uuid.uuid4())
        payload = {
            "action": "delay",
            "task_id": task_id,
            "data": data,
            "queue_ttl": int(queue_ttl.total_seconds()),
            "task_ttl": int(task_ttl.total_seconds()),
            "timestamp": time.time()
        }
        try:
            response = await self._nc.request(subject, json.dumps(payload).encode(), timeout=30)
            resp_text = response.data.decode()
            if self.debug:
                print(f"[AsyncEdaliteCaller] Delay response: {resp_text}")
            return task_id
        except Exception as e:
            raise RuntimeError(f"Delay request failed: {e}")

    async def get(self, subject: str, task_id: str, timeout: float = 30.0) -> dict:
        """
        Retrieve the result of a deferred task via NATS request/reply.

        Parameters
        ----------
        subject : str
            The NATS subject.
        task_id : str
            The task identifier.
        timeout : float, optional
            Request timeout in seconds.

        Returns
        -------
        dict
            The task result (as a dict) or a message.
        """
        payload = {
            "action": "get",
            "task_id": task_id
        }
        try:
            response = await self._nc.request(subject, json.dumps(payload).encode(), timeout=timeout)
            resp_text = response.data.decode()
            if self.debug:
                print(f"[AsyncEdaliteCaller] Get response: {resp_text}")
            try:
                data = json.loads(resp_text)
                return data
            except Exception:
                return {"message": resp_text}
        except Exception as e:
            raise RuntimeError(f"Get request failed: {e}")

    async def close(self):
        """
        Asynchronously close the NATS and Redis connections.
        """
        await self._nc.close()
        await self._redis.close()
        if self.debug:
            print("[AsyncEdaliteCaller] Connections closed.")
