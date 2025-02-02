#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Edalite Worker and Caller Implementation using NATS and Redis KV Store
======================================================================

이 모듈은 NATS 메시징 시스템과 Redis KV 스토어를 이용하여,
즉시 실행과 지연(백그라운드) 실행을 하나의 작업 등록 방식으로 지원합니다.
클라이언트는 요청 시 JSON 페이로드에 "mode" 필드로 요청 종류를 명시합니다.
예를 들어, "immediate" 모드로 호출하면 작업이 완료된 후 결과를 응답하고,
"deferred" 모드로 호출하면 caller 쪽에서 생성한 task_id를 응답한 후 작업 결과는 Redis에 저장됩니다.
저장되는 작업 상태는 다음과 같이 진행됩니다:
    - 초기 (caller 쪽에서 저장): QUEUED
    - 작업 시작 시 (worker에서 업데이트): PROCESSING
    - 작업 성공 시 (worker에서 업데이트): COMPLETED
    - 작업 실패 시 (worker에서 업데이트): FAILURE

클라이언트와 워커는 각각 동기/비동기 방식으로 사용할 수 있으며, Redis 및 NATS 연결은
각각의 방식에 맞게 초기화됩니다.

Classes
-------
EdaliteWorker
    단일 데코레이터(@task)로 즉시/지연 작업을 등록하며, 클라이언트의 요청 페이로드("mode")
    에 따라 조건문으로 즉시 실행 또는 백그라운드 실행(지연)을 수행합니다.
EdaliteCaller
    동기 클라이언트로, request()와 delay() 호출 시 각각 "immediate"와 "deferred" 모드의
    페이로드를 보내어 작업을 요청하며, Redis를 통해 작업 상태를 확인합니다.
AsyncEdaliteCaller
    비동기 클라이언트로, request()와 delay() 호출 시 각각 "immediate"와 "deferred" 모드의
    페이로드를 보내어 작업을 요청합니다.
"""

import asyncio
import uuid
import json
import threading
import time
from typing import Callable, Any, Union, List
import os

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NATSMessage

from redis import Redis
from redis.asyncio import Redis as RedisAsync

import concurrent.futures
import multiprocessing


##############################################################################
# Synchronous Worker (Server) Implementation using Redis for KV Storage
##############################################################################
class EdaliteWorker:
    """
    Synchronous worker that registers functions for immediate and deferred execution
    via NATS messaging. Depending on the client request payload ("mode"), the registered
    task is executed immediately (responding with the result) or deferred (the worker
    updates task state in Redis from PROCESSING to COMPLETED/FAILURE).

    Deferred tasks update their state in Redis:
        - When processing starts: PROCESSING
        - On success: COMPLETED
        - On failure: FAILURE
    (Caller already stores the initial QUEUED state.)
    """

    def __init__(
        self,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
        max_thread: int = 1,
        max_process: int = 1,
    ):
        self.nats_url = nats_url
        self.redis_url = redis_url
        self.debug = debug

        self.max_thread = max_thread
        self.max_process = max_process

        # Register tasks with a single decorator (TTL 관련 코드는 사용하지 않음)
        self.tasks = {}

        self._nc = None
        self._redis = None
        self.loop = None
        self.task_executor = None  # Determined at runtime

        # Multiprocessing context for spawning processes
        self._mp_context = multiprocessing.get_context("spawn")

    def task(
        self,
        subject: str,
        queue_group: str = None,
    ) -> Callable:
        """
        Decorator to register a task function for a given subject.

        The client sends a JSON payload with "mode" set to "immediate" or "deferred".
        For deferred mode, the caller already stores the initial state (QUEUED) in Redis.
        The worker then updates the state:
            - PROCESSING: when starting execution
            - COMPLETED: on success
            - FAILURE: on error

        Parameters
        ----------
        subject : str
            Subject to listen on.
        queue_group : str, optional
            Queue group for load balancing.

        Returns
        -------
        Callable
            The decorator function.
        """

        def decorator(func: Callable):
            if subject not in self.tasks:
                self.tasks[subject] = []
            for existing_func, existing_queue in self.tasks[subject]:
                if existing_queue == queue_group:
                    raise ValueError(
                        f"Queue group '{queue_group}' is already registered for subject '{subject}'"
                    )
            self.tasks[subject].append((func, queue_group))
            return func

        return decorator

    def start(self):
        """
        Start the worker to listen for messages on registered subjects.

        If max_process > 1, multiple processes are spawned; each creates its own
        asyncio event loop, NATS, and Redis connections.
        """
        if self.max_process > 1:
            print("---------------------------------------")
            print(f"Starting {self.max_process} processes... (PID: {os.getpid()})")
            print("---------------------------------------")

            processes = []
            for _ in range(self.max_process):
                p = self._mp_context.Process(target=self._run_in_new_process)
                p.start()
                processes.append(p)

            # 단순히 모든 프로세스를 대기 (join)시켜 종료되지 않도록 함
            for p in processes:
                p.join()
        else:
            self._start_single()

    def _run_in_new_process(self):
        """Logic executed in a new process for multiprocessing."""
        self._start_single()

    def _start_single(self):
        """Actual worker logic for a single process."""
        self.loop = asyncio.new_event_loop()
        loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        loop_thread.start()

        if self.max_thread > 1:
            self.task_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_thread
            )
        else:
            self.task_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        future = asyncio.run_coroutine_threadsafe(self._init_nats(), self.loop)
        try:
            future.result()  # Wait until NATS and Redis are initialized
        except Exception as e:
            if self.debug:
                print(f"Error during NATS initialization: {e}")
            return

        if self.debug:
            if self.max_process > 1:
                print(
                    f"EdaliteWorker is now running (multi process). (PID: {os.getpid()})"
                )
            else:
                print(
                    f"EdaliteWorker is now running (single process). (PID: {os.getpid()})"
                )
        print("================================================")
        try:
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
        Initialize NATS and Redis connections, and set up subscriptions for tasks.
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
            print(f"Connected to NATS server (PID: {os.getpid()})")
        self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        if self.debug:
            print(f"Connected to Redis server at {self.redis_url} (PID: {os.getpid()})")
        # Set up subscriptions for each registered task
        for subject, handlers in self.tasks.items():
            for task, queue_group in handlers:

                async def callback(msg: NATSMessage, f=task):
                    try:
                        req = json.loads(msg.data.decode())
                    except Exception as e:
                        await msg.respond(f"Error: invalid payload: {e}".encode())
                        return
                    mode = req.get("mode", "immediate")
                    data = req.get("data")
                    if data is None:
                        await msg.respond("Error: missing 'data' field".encode())
                        return
                    if mode == "immediate":
                        self._handle_unified_immediate(msg, f, data)
                    elif mode == "deferred":
                        task_id = req.get("task_id")
                        if not task_id:
                            await msg.respond(
                                "Error: missing task_id in deferred request".encode()
                            )
                            return
                        self._handle_unified_deferred(msg, f, data, task_id)
                    else:
                        await msg.respond(f"Error: unknown mode '{mode}'".encode())

                if queue_group:
                    await self._nc.subscribe(subject, cb=callback, queue=queue_group)
                else:
                    await self._nc.subscribe(subject, cb=callback)
        if self.debug:
            print("Subscriptions set up for subjects:")
            print("  Tasks:", list(self.tasks.keys()))
            print(f"(PID: {os.getpid()})")

    @staticmethod
    def _execute_task(task: Callable, data: Any):
        """Helper method to execute a task."""
        return task(data)

    def _respond_immediate(self, msg: NATSMessage, future: concurrent.futures.Future):
        """Callback to send the result of an immediate task."""
        try:
            result = future.result()
            if self.debug:
                print(f"[Immediate] Sending response: {result} (PID: {os.getpid()})")
            asyncio.run_coroutine_threadsafe(
                msg.respond(str(result).encode()), self.loop
            )
        except Exception as e:
            if self.debug:
                print(f"[Immediate] Error: {e} (PID: {os.getpid()})")
            asyncio.run_coroutine_threadsafe(
                msg.respond(f"Error: {str(e)}".encode()), self.loop
            )

    def _handle_unified_immediate(self, msg: NATSMessage, task: Callable, data: Any):
        """Handle immediate execution requests (mode 'immediate')."""
        if self.debug:
            print(f"[Immediate] Received: {data} (PID: {os.getpid()})")
        if self.max_thread > 1:
            future = self.task_executor.submit(self._execute_task, task, data)
            future.add_done_callback(lambda f: self._respond_immediate(msg, f))
        else:
            try:
                result = self._execute_task(task, data)
                if self.debug:
                    print(
                        f"[Immediate] Sending response: {result} (PID: {os.getpid()})"
                    )
                asyncio.run_coroutine_threadsafe(
                    msg.respond(str(result).encode()), self.loop
                )
            except Exception as e:
                if self.debug:
                    print(f"[Immediate] Error: {e} (PID: {os.getpid()})")
                asyncio.run_coroutine_threadsafe(
                    msg.respond(f"Error: {str(e)}".encode()), self.loop
                )

    def _handle_unified_deferred(
        self, msg: NATSMessage, task: Callable, data: Any, task_id: str
    ):
        """Handle deferred execution requests (mode 'deferred')."""
        # Respond immediately with ACK (caller already generated task_id and stored QUEUED)
        # asyncio.run_coroutine_threadsafe(msg.respond("ACK".encode()), self.loop)
        if self.debug:
            print(
                f"[Deferred] Received: {data}, task_id={task_id} (PID: {os.getpid()})"
            )
        subject = msg.subject
        # Update state to PROCESSING
        self._publish_deferred_status(subject, task_id, "PROCESSING", None)
        if self.max_thread > 1:
            future = self.task_executor.submit(self._execute_task, task, data)
            future.add_done_callback(
                lambda f: self._handle_deferred_callback(f, subject, task_id)
            )
        else:
            try:
                result = self._execute_task(task, data)
                self._publish_deferred_status(subject, task_id, "COMPLETED", result)
                if self.debug:
                    print(
                        f"[Deferred] Task {task_id} completed with result: {result} (PID: {os.getpid()})"
                    )
            except Exception as e:
                self._publish_deferred_status(subject, task_id, "FAILURE", str(e))
                if self.debug:
                    print(f"[Deferred] Task {task_id} failed: {e} (PID: {os.getpid()})")

    def _handle_deferred_callback(
        self,
        future: concurrent.futures.Future,
        subject: str,
        task_id: str,
    ):
        """Callback to update deferred task status in Redis after execution."""
        try:
            result = future.result()
            self._publish_deferred_status(subject, task_id, "COMPLETED", result)
            if self.debug:
                print(
                    f"[Deferred] Task {task_id} completed with result: {result} (PID: {os.getpid()})"
                )
        except Exception as e:
            self._publish_deferred_status(subject, task_id, "FAILURE", str(e))
            if self.debug:
                print(f"[Deferred] Task {task_id} failed: {e} (PID: {os.getpid()})")

    def _publish_deferred_status(
        self, subject: str, task_id: str, status: str, result: Any
    ):
        """
        Publish the status and result of a deferred task by storing it in Redis.
        Redis key format: DEFERRED_TASKS_{subject}:{task_id}
        """
        redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
        doc = {
            "task_id": task_id,
            "status": status,
            "result": result,
        }
        try:
            self._redis.set(redis_key, json.dumps(doc))
        except Exception as e:
            if self.debug:
                print(f"[Redis] Error storing deferred status for {redis_key}: {e}")


##############################################################################
# Synchronous Client (Caller) Implementation using Redis for KV Storage
##############################################################################
class EdaliteCaller:
    """
    Synchronous client to call immediate or deferred functions registered on an
    EdaliteWorker via NATS messaging. The client sends a JSON payload with a "mode"
    field ("immediate" or "deferred") and "data", and deferred task results are
    retrieved from Redis. Deferred task states progress as:
        QUEUED -> PROCESSING -> COMPLETED (or FAILURE)
    """

    def __init__(
        self,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
    ):
        self.nats_url = nats_url
        self.redis_url = redis_url
        self.debug = debug

        self._nc = None  # NATS client (asynchronous)
        self._redis = None  # Redis synchronous client
        self.loop = None  # Background asyncio event loop

    def connect(self) -> "EdaliteCaller":
        """
        Connect to the NATS server and initialize the Redis client.
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
        Initialize the NATS and Redis clients.
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

    def request(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """
        Call an immediate execution function and return the result synchronously.
        Payload: {"mode": "immediate", "data": <data>}
        """
        try:
            payload = json.dumps({"mode": "immediate", "data": str(data)})
            future = asyncio.run_coroutine_threadsafe(
                self._nc.request(subject, payload.encode(), timeout=timeout),
                self.loop,
            )
            response = future.result(timeout)
            result = response.data.decode()
            if result.startswith("Error:"):
                raise RuntimeError(result[6:].strip())
            return result
        except Exception as e:
            raise RuntimeError(f"Function call failed ({subject}): {e}")

    def delay(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """
        Call a deferred execution function.
        Payload: {"mode": "deferred", "data": <data>, "task_id": <generated_id>}

        The caller generates a task_id and stores the initial state (QUEUED) in Redis.
        Then it publishes the payload via NATS without waiting for an ACK.
        The worker will update the state to PROCESSING and eventually to COMPLETED (or FAILURE).
        """
        try:
            # Generate task_id and store initial state QUEUED in Redis
            task_id = str(uuid.uuid4())
            redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
            doc = {"task_id": task_id, "status": "QUEUED", "result": None}
            try:
                self._redis.set(redis_key, json.dumps(doc))
            except Exception as e:
                if self.debug:
                    print(f"Error storing initial deferred status: {e}")
            payload = json.dumps(
                {"mode": "deferred", "data": str(data), "task_id": task_id}
            )
            # Use publish instead of request so as to return task_id immediately
            asyncio.run_coroutine_threadsafe(
                self._nc.publish(subject, payload.encode()), self.loop
            ).result()
            return task_id
        except Exception as e:
            raise RuntimeError(f"Deferred function call failed ({subject}): {e}")

    def get_deferred_result(self, subject: str, task_id: str) -> dict:
        """
        Retrieve the result of a deferred task from Redis.
        Redis key format: DEFERRED_TASKS_{subject}:{task_id}
        The returned dict contains 'task_id', 'status', and 'result'.
        """
        redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
        try:
            value = self._redis.get(redis_key)
            if value is None:
                return {"error": f"No KV data found for task_id={task_id}"}
            data = json.loads(value)
            return data
        except Exception as e:
            return {"error": str(e)}

    def close(self):
        """
        Close the NATS connection and stop the background asyncio event loop.
        """
        asyncio.run_coroutine_threadsafe(self._nc.close(), self.loop).result()
        self.loop.call_soon_threadsafe(self.loop.stop)


##############################################################################
# Asynchronous Client (Caller) Implementation using Redis for KV Storage
##############################################################################
class AsyncEdaliteCaller:
    """
    Asynchronous client to call immediate or deferred functions registered on an
    EdaliteWorker via NATS messaging. The client sends a JSON payload with "mode"
    ("immediate" or "deferred") and "data" (and "task_id" for deferred mode),
    and deferred task results are retrieved from Redis asynchronously.
    """

    def __init__(
        self,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
    ):
        self.nats_url = nats_url
        self.redis_url = redis_url
        self.debug = debug

        self._nc = None  # NATS client (asynchronous)
        self._redis = None  # Redis asynchronous client
        self.loop = asyncio.get_event_loop()

    @classmethod
    async def connect(
        cls,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
    ) -> "AsyncEdaliteCaller":
        """
        Create an instance of AsyncEdaliteCaller and connect to NATS and Redis.
        """
        instance = cls(nats_url, redis_url, debug)
        instance._nc = NATS()
        servers = (
            instance.nats_url
            if isinstance(instance.nats_url, list)
            else [instance.nats_url]
        )
        await instance._nc.connect(
            servers=servers,
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
            ping_interval=20,
            max_outstanding_pings=5,
        )
        instance._redis = RedisAsync.from_url(instance.redis_url, decode_responses=True)
        if debug:
            print("AsyncEdaliteCaller connected.")
        return instance

    async def request(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """
        Call an immediate execution function asynchronously.
        Payload: {"mode": "immediate", "data": <data>}
        """
        payload = json.dumps({"mode": "immediate", "data": str(data)})
        response = await self._nc.request(subject, payload.encode(), timeout=timeout)
        result = response.data.decode()
        if result.startswith("Error:"):
            raise RuntimeError(result[6:].strip())
        return result

    async def delay(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """
        Call a deferred execution function asynchronously.
        Payload: {"mode": "deferred", "data": <data>, "task_id": <generated_id>}

        The caller generates a task_id and stores the initial state (QUEUED) in Redis.
        Then it publishes the payload via NATS without waiting for an ACK.
        The worker will update the state to PROCESSING and eventually to COMPLETED (or FAILURE).
        """
        task_id = str(uuid.uuid4())
        redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
        doc = {"task_id": task_id, "status": "QUEUED", "result": None}
        try:
            await self._redis.set(redis_key, json.dumps(doc))
        except Exception as e:
            if self.debug:
                print(f"Error storing initial deferred status: {e}")
        payload = json.dumps(
            {"mode": "deferred", "data": str(data), "task_id": task_id}
        )
        await self._nc.publish(subject, payload.encode())
        return task_id

    async def get_deferred_result(self, subject: str, task_id: str) -> dict:
        """
        Retrieve the result of a deferred task from Redis asynchronously.
        """
        redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
        try:
            value = await self._redis.get(redis_key)
            if value is None:
                return {"error": f"No KV data found for task_id={task_id}"}
            data = json.loads(value)
            return data
        except Exception as e:
            return {"error": str(e)}

    async def close(self):
        """
        Close the NATS and Redis connections asynchronously.
        """
        await self._nc.close()
        await self._redis.close()

    async def subscribe(self, subject: str, callback: Callable):
        """
        Subscribe to a subject for broadcast messages.
        """
        await self._nc.subscribe(subject, cb=callback)
