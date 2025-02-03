#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Edalite Worker and Caller Implementation using NATS and Redis KV Store
======================================================================

This module provides an implementation of a worker (server) and clients (callers)
that handle immediate and deferred function execution via a NATS messaging system.
Deferred task results are stored in Redis. The module supports both synchronous and
asynchronous clients.

The NATS URL parameter can be provided as either a single URL string or a list of URL
strings for cluster configurations.

Redis connections are handled via:
    from redis import Redis
    from redis.asyncio import Redis as RedisAsync

For synchronous operations, a Redis client is created as follows:
    redis_proxy_sync = Redis(host=<host>, port=<port>, db=<db_sync>, decode_responses=True)

For asynchronous operations, a Redis client is created as follows:
    redis_proxy_async = RedisAsync(host=<host>, port=<port>, db=<db_async>, decode_responses=True)

Classes
-------
EdaliteWorker
    Synchronous worker that registers functions for immediate and deferred execution.
    It supports choosing an executor mode ("thread" or "process") for task processing.
EdaliteCaller
    Synchronous client to call immediate or deferred functions on the worker.
AsyncEdaliteCaller
    Asynchronous client to call immediate or deferred functions on the worker.

Examples
--------
Synchronous example:

    from datetime import timedelta
    import time
    import threading
    from edalite_module import EdaliteWorker, EdaliteCaller

    # Create and register worker functions with executor mode "thread" or "process"
    worker = EdaliteWorker(
        nats_url="nats://localhost:4222",
        redis_url="redis://localhost:6379/0",
        executor="process",  # choose "thread" or "process"
        debug=True
    )

    @worker.task("service.immediate", queue_group="group1")
    def immediate_task(data):
        print(f"Immediate task received: {data}")
        return f"Processed immediate data: {data}"

    @worker.delayed_task("service.deferred", queue_group="group2", ttl=timedelta(seconds=60))
    def deferred_task(data):
        print(f"Deferred task processing: {data}")
        import time
        time.sleep(2)
        return f"Processed deferred data: {data}"

    # Start the worker in a separate thread
    threading.Thread(target=worker.start, daemon=True).start()
    time.sleep(1)

    # Create a synchronous caller
    caller = EdaliteCaller(
        nats_url="nats://localhost:4222",
        redis_url="redis://localhost:6379/0",
        debug=True
    ).connect()

    # Call immediate function
    result_immediate = caller.request("service.immediate", "Hello Immediate!")
    print(f"Immediate response: {result_immediate}")

    # Call deferred function
    task_id = caller.delay("service.deferred", "Hello Deferred!")
    print(f"Deferred task_id: {task_id}")

    # Wait for deferred task completion and then fetch its result
    time.sleep(3)
    deferred_result = caller.get_deferred_result("service.deferred", task_id)
    print(f"Deferred task result: {deferred_result}")

    # Clean up
    caller.close()


Asynchronous example:

    import asyncio
    from edalite_module import AsyncEdaliteCaller

    async def async_main():
        caller = await AsyncEdaliteCaller.connect(
            nats_url="nats://localhost:4222",
            redis_url="redis://localhost:6379/1",
            debug=True
        )
        result = await caller.request("service.immediate", "Async Hello Immediate!")
        print(f"Async Immediate response: {result}")
        task_id = await caller.delay("service.deferred", "Async Hello Deferred!")
        print(f"Async Deferred task_id: {task_id}")
        await asyncio.sleep(3)
        deferred_result = await caller.get_deferred_result("service.deferred", task_id)
        print(f"Async Deferred task result: {deferred_result}")
        await caller.close()

    asyncio.run(async_main())
"""

import asyncio
import uuid
import json
import threading
import time
from datetime import timedelta
from typing import Callable, Any, Union, List
import os  # 추가: PID를 가져오기 위해 os 모듈을 임포트

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NATSMessage

from redis import Redis
from redis.asyncio import Redis as RedisAsync

import concurrent.futures


##############################################################################
# Synchronous Worker (Server) Implementation using Redis for KV Storage
##############################################################################
class EdaliteWorker:
    """
    Synchronous worker that registers functions for immediate and deferred execution
    via NATS messaging. Deferred task results are stored in Redis.

    This worker supports selecting an executor mode for task processing:
      - "thread": Uses a ThreadPoolExecutor (multithreading)
      - "process": Uses a ProcessPoolExecutor (multiprocessing, each process is single-threaded)

    Parameters
    ----------
    nats_url : Union[str, List[str]], optional
        The URL or list of URLs of the NATS server(s). Default is "nats://localhost:4222".
    redis_url : str, optional
        The URL of the Redis server. Default is "redis://localhost:6379/0".
    debug : bool, optional
        If True, prints debug information. Default is False.
    max_thread : int, optional
        If 1이면 싱글 스레드로 실행하고, 2 이상이면 ThreadPoolExecutor로 실행합니다.
    max_process : int, optional
        If 1이면 프로세스 분할 없이 그대로 실행하고, 2 이상이면 start() 호출 시
        현재 클래스를 여러 프로세스로 띄워줍니다. Default는 1입니다.

    Attributes
    ----------
    functions : dict
        Dictionary mapping subjects to a list of tuples (function, queue_group)
        for immediate execution.
    deferred_functions : dict
        Dictionary mapping subjects to a list of tuples (function, queue_group, ttl)
        for deferred execution.
    _nc : NATS
        The asynchronous NATS client instance.
    _redis : Redis
        The synchronous Redis client instance used to store deferred task results.
    loop : asyncio.AbstractEventLoop
        The asyncio event loop running in a separate thread.
    task_executor : Executor or None
        필요한 경우 스레드 풀 혹은 프로세스 풀을 사용합니다. (max_thread가 2 이상일 경우 ThreadPoolExecutor)
        (max_process가 1이면 기존 방식, 2 이상이면 여러 프로세스에서 이 클래스를 실행)
    """

    def __init__(
        self,
        nats_url: Union[str, List[str]] = "nats://localhost:4222",
        redis_url: str = "redis://localhost:6379/0",
        debug: bool = False,
        max_thread: int = 1,
        max_process: int = 1,
    ):
        import multiprocessing

        self.nats_url = nats_url
        self.redis_url = redis_url
        self.debug = debug

        # max_thread: 1이면 싱글 스레드, 2 이상이면 ThreadPoolExecutor 사용
        self.max_thread = max_thread
        # max_process: 1이면 프로세스 분할 없이 실행, 2 이상이면 여러 프로세스로 이 클래스를 실행
        self.max_process = max_process

        self.functions = {}
        self.deferred_functions = {}

        self._nc = None
        self._redis = None
        self.loop = None
        self.task_executor = None  # 실행 시점에서 결정

        # 추가: 멀티프로세스 시 사용하기 위한 Process 클래스용
        self._mp_context = multiprocessing.get_context("spawn")

    def task(self, subject: str, queue_group: str = None) -> Callable:
        """
        Decorator to register an immediate execution task.

        The decorated function is executed via the selected executor when a message is received
        on the specified subject.

        Parameters
        ----------
        subject : str
            The subject (channel) on which the message will be received.
        queue_group : str, optional
            The queue group name for load balancing. Default is None.

        Returns
        -------
        Callable
            The decorator function.

        Raises
        ------
        ValueError
            If the same queue_group is already registered for the subject.
        """

        def decorator(task: Callable):
            if subject not in self.functions:
                self.functions[subject] = []
            for existing_task, existing_queue in self.functions[subject]:
                if existing_queue == queue_group:
                    raise ValueError(
                        f"Queue group '{queue_group}' is already registered for subject '{subject}'"
                    )
            self.functions[subject].append((task, queue_group))
            return task

        return decorator

    def delayed_task(
        self, subject: str, queue_group: str = None, ttl: timedelta = timedelta(days=1)
    ) -> Callable:
        """
        Decorator to register a deferred (background) execution task.

        Upon receiving a message, the worker responds immediately with a generated
        task_id while processing the task in the background. The result is stored in
        Redis with the specified time-to-live (TTL).

        Parameters
        ----------
        subject : str
            The subject (channel) on which the message will be received.
        queue_group : str, optional
            The queue group name for load balancing. Default is None.
        ttl : timedelta, optional
            The time-to-live for the Redis key storing the task result. Default is 1 day.

        Returns
        -------
        Callable
            The decorator function.

        Raises
        ------
        ValueError
            If the same queue_group is already registered for the subject.
        """

        def decorator(task: Callable):
            if subject not in self.deferred_functions:
                self.deferred_functions[subject] = []
            for existing_task, existing_queue, _ in self.deferred_functions.get(
                subject, []
            ):
                if existing_queue == queue_group:
                    raise ValueError(
                        f"Queue group '{queue_group}' is already registered for subject '{subject}'"
                    )
            self.deferred_functions[subject].append((task, queue_group, ttl))
            return task

        return decorator

    def start(self):
        """
        Start the worker to listen for messages on registered subjects.

        이 메서드는 아래 단계를 수행합니다:
            1. max_process가 1 초과라면, 여러 프로세스를 생성하여 이 클래스를 병렬로 실행합니다.
               (실행 후, 각 프로세스가 별도로 start() 로직을 수행하므로 주의)
            2. 프로세스가 1개뿐이라면, asyncio 이벤트 루프를 스레드로 시작하고
               NATS, Redis에 연결한 뒤, (max_thread에 따라) ThreadPoolExecutor 등을 구성합니다.
            3. 메인 스레드를 유지하면서 메시지를 계속 수신, 처리합니다.
        """
        # 여러 프로세스를 돌릴 경우
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
            # 프로세스가 1개라면 기존 로직대로 실행
            self._start_single()

    def _run_in_new_process(self):
        """
        멀티프로세스 환경에서, 새 프로세스가 수행할 로직을 담은 내부 함수입니다.
        이 함수는 self.start()에서 여러 번 호출될 수 있습니다.
        """
        self._start_single()

    def _start_single(self):
        """
        하나의 프로세스(또는 싱글 프로세스)에서 실제로 워커 로직을 수행하는 부분입니다.
        """
        self.loop = asyncio.new_event_loop()
        loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        loop_thread.start()

        # ThreadPoolExecutor 설정
        if self.max_thread > 1:
            self.task_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_thread
            )
        else:
            # 싱글 스레드 상태: ThreadPoolExecutor를 사용하지 않고 직접 실행
            # 필요에 따라 별도의 로직을 추가할 수도 있지만,
            # 여기서는 그냥 ThreadPoolExecutor에 워커 수 1로 설정하는 방식을 사용
            self.task_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        future = asyncio.run_coroutine_threadsafe(self._init_nats(), self.loop)
        try:
            future.result()  # Wait until initialization completes
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
        """
        Internal method to run the asyncio event loop in a separate thread.
        """
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _init_nats(self):
        """
        Asynchronously initialize the NATS connection and the Redis client,
        and set up subscriptions for immediate and deferred tasks.

        For NATS, if the nats_url is provided as a list, it connects to all the
        specified servers.

        For Redis, a synchronous client is created using the provided connection
        parameters.
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

        # Initialize Redis synchronous client using from_url
        self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        if self.debug:
            print(f"Connected to Redis server at {self.redis_url} (PID: {os.getpid()})")

        # Set up subscriptions for immediate execution tasks
        for subject, handlers in self.functions.items():
            for task, queue_group in handlers:

                async def callback(msg: NATSMessage, f=task):
                    self._handle_message(msg, f)

                if queue_group:
                    await self._nc.subscribe(subject, cb=callback, queue=queue_group)
                else:
                    await self._nc.subscribe(subject, cb=callback)

        # Set up subscriptions for deferred execution tasks
        for subject, handlers in self.deferred_functions.items():
            for task, queue_group, ttl in handlers:

                async def callback(msg: NATSMessage, f=task, used_ttl=ttl):
                    self._handle_deferred_message(msg, f, used_ttl)

                if queue_group:
                    await self._nc.subscribe(subject, cb=callback, queue=queue_group)
                else:
                    await self._nc.subscribe(subject, cb=callback)

        if self.debug:
            print("Subscriptions set up for subjects:")
            print("  Immediate:", list(self.functions.keys()))
            print("  Deferred:", list(self.deferred_functions.keys()))
            print(f"(PID: {os.getpid()})")

    @staticmethod
    def _execute_task(task: Callable, data: Any):
        """
        Helper method to execute a task with given data.

        싱글 스레드 모드에서도 동일하게 호출되며, ThreadPoolExecutor가 있으면
        풀에 제출되어 실행됩니다.
        """
        return task(data)

    def _respond_immediate(self, msg: NATSMessage, future: concurrent.futures.Future):
        """
        Callback to respond to an immediate task once execution completes.

        Parameters
        ----------
        msg : NATSMessage
            The original NATS message.
        future : concurrent.futures.Future
            The future representing the task execution.
        """
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

    def _handle_message(self, msg: NATSMessage, task: Callable):
        """
        Handle an immediate execution message by submitting the task to
        the selected executor or running directly if in single-thread mode.

        Parameters
        ----------
        msg : NATSMessage
            The received NATS message.
        task : Callable
            The user-defined function to process the message.
        """
        data = msg.data.decode()
        if self.debug:
            print(f"[Immediate] Received: {data} (PID: {os.getpid()})")

        if self.max_thread > 1:
            # ThreadPoolExecutor 사용
            future = self.task_executor.submit(self._execute_task, task, data)
            future.add_done_callback(lambda f: self._respond_immediate(msg, f))
        else:
            # 싱글 스레드 실행
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

    def _handle_deferred_message(
        self, msg: NATSMessage, task: Callable, ttl: timedelta
    ):
        """
        Handle a deferred execution message by submitting the task
        to the executor or running directly if in single-thread mode.

        Parameters
        ----------
        msg : NATSMessage
            The received NATS message.
        task : Callable
            The function to process the deferred message.
        ttl : timedelta
            The time-to-live for the Redis key storing the task result.
        """
        task_id = str(uuid.uuid4())
        asyncio.run_coroutine_threadsafe(msg.respond(task_id.encode()), self.loop)
        data = msg.data.decode()
        if self.debug:
            print(
                f"[Deferred] Received: {data}, task_id={task_id} (PID: {os.getpid()})"
            )
        subject = msg.subject
        self._publish_deferred_status(subject, task_id, "pending", None, ttl)

        if self.max_thread > 1:
            future = self.task_executor.submit(self._execute_task, task, data)
            future.add_done_callback(
                lambda f: self._handle_deferred_callback(f, subject, task_id, ttl)
            )
        else:
            # 싱글 스레드 실행
            try:
                result = self._execute_task(task, data)
                self._publish_deferred_status(
                    subject, task_id, "completed", result, ttl
                )
                if self.debug:
                    print(
                        f"[Deferred] Task {task_id} completed with result: {result} (PID: {os.getpid()})"
                    )
            except Exception as e:
                self._publish_deferred_status(subject, task_id, "error", str(e), ttl)
                if self.debug:
                    print(f"[Deferred] Task {task_id} failed: {e} (PID: {os.getpid()})")

    def _handle_deferred_callback(
        self,
        future: concurrent.futures.Future,
        subject: str,
        task_id: str,
        ttl: timedelta,
    ):
        """
        Callback to update the deferred task status in Redis once execution completes.

        Parameters
        ----------
        future : concurrent.futures.Future
            The future representing the deferred task execution.
        subject : str
            The subject associated with the deferred task.
        task_id : str
            The unique identifier for the task.
        ttl : timedelta
            The time-to-live for the Redis key storing the task result.
        """
        try:
            result = future.result()
            self._publish_deferred_status(subject, task_id, "completed", result, ttl)
            if self.debug:
                print(
                    f"[Deferred] Task {task_id} completed with result: {result} (PID: {os.getpid()})"
                )
        except Exception as e:
            self._publish_deferred_status(subject, task_id, "error", str(e), ttl)
            if self.debug:
                print(f"[Deferred] Task {task_id} failed: {e} (PID: {os.getpid()})")

    def _publish_deferred_status(
        self, subject: str, task_id: str, status: str, result: Any, ttl: timedelta
    ):
        """
        Publish the status and result of a deferred task by storing it in Redis.

        The Redis key format is: DEFERRED_TASKS_{subject}:{task_id}
        The key is set with an expiration corresponding to the provided TTL.

        Parameters
        ----------
        subject : str
            The subject associated with the deferred task.
        task_id : str
            The unique identifier for the task.
        status : str
            The current status of the task ('pending', 'completed', or 'error').
        result : Any
            The result of the task execution or error message.
        ttl : timedelta
            The time-to-live for the Redis key.
        """
        redis_key = f"DEFERRED_TASKS_{subject.replace('.', '_')}:{task_id}"
        doc = {
            "task_id": task_id,
            "status": status,
            "result": result,
        }
        try:
            self._redis.set(redis_key, json.dumps(doc), ex=int(ttl.total_seconds()))
        except Exception as e:
            if self.debug:
                print(f"[Redis] Error storing deferred status for {redis_key}: {e}")


##############################################################################
# Synchronous Client (Caller) Implementation using Redis for KV Storage
##############################################################################
class EdaliteCaller:
    """
    Synchronous client to call immediate or deferred functions registered on an
    EdaliteWorker via NATS messaging. Deferred task results are retrieved from Redis.

    Parameters
    ----------
    nats_url : Union[str, List[str]], optional
        The URL or list of URLs of the NATS server(s). Default is "nats://localhost:4222".
    redis_url : str, optional
        The URL of the Redis server. Default is "redis://localhost:6379/0".
    debug : bool, optional
        If True, prints debug information. Default is False.

    Attributes
    ----------
    _nc : NATS
        The asynchronous NATS client instance.
    _redis : Redis
        The synchronous Redis client instance used to retrieve deferred task results.
    loop : asyncio.AbstractEventLoop
        The asyncio event loop running in a separate thread.
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

        This method starts a background asyncio event loop in a separate thread,
        initializes the asynchronous NATS client, and creates a synchronous Redis
        client for deferred task result retrieval.

        Returns
        -------
        EdaliteCaller
            The instance itself after successful connection.

        Examples
        --------
        >>> caller = EdaliteCaller(nats_url="nats://localhost:4222",
        ...                        redis_url="redis://localhost:6379/0",
        ...                        debug=True).connect()
        >>> result = caller.request("service.immediate", "Hello!")
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
        Internal method to run the asyncio event loop in a background thread.
        """
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _init_nats(self):
        """
        Asynchronously initialize the NATS client and the Redis synchronous client.

        For NATS, if nats_url is provided as a list, it connects to all specified servers.
        For Redis, the synchronous client is created using the provided connection parameters.
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
        # Initialize Redis synchronous client using from_url
        self._redis = Redis.from_url(self.redis_url, decode_responses=True)

    def request(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """
        Call an immediate execution function and wait synchronously for a response.

        Parameters
        ----------
        subject : str
            The subject to which the request is sent.
        data : Any
            The data to be sent. It will be converted to a string.
        timeout : float, optional
            The request timeout in seconds. Default is 30.0.

        Returns
        -------
        str
            The response returned by the worker.

        Raises
        ------
        RuntimeError
            If the function call fails or returns an error.

        Examples
        --------
        >>> result = caller.request("service.immediate", "Hello!")
        >>> print("Response:", result)
        """
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._nc.request(subject, str(data).encode(), timeout=timeout),
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

        This method sends a request to the worker and returns immediately with a
        generated task_id. The actual processing is done in the background by the worker,
        and the result is stored in Redis.

        Parameters
        ----------
        subject : str
            The subject to which the request is sent.
        data : Any
            The data to be sent. It will be converted to a string.
        timeout : float, optional
            The request timeout in seconds. Default is 30.0.

        Returns
        -------
        str
            The task_id assigned by the worker.

        Raises
        ------
        RuntimeError
            If the deferred function call fails or returns an error.

        Examples
        --------
        >>> task_id = caller.delay("service.deferred", "Hello Deferred!")
        >>> print("Task ID:", task_id)
        """
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._nc.request(subject, str(data).encode(), timeout=timeout),
                self.loop,
            )
            response = future.result(timeout)
            task_id = response.data.decode()
            if task_id.startswith("Error:"):
                raise RuntimeError(task_id[6:].strip())
            return task_id
        except Exception as e:
            raise RuntimeError(f"Deferred function call failed ({subject}): {e}")

    def get_deferred_result(self, subject: str, task_id: str) -> dict:
        """
        Retrieve the result of a deferred function from Redis.

        The Redis key format is: DEFERRED_TASKS_{subject}:{task_id}

        Parameters
        ----------
        subject : str
            The subject corresponding to the deferred function.
        task_id : str
            The task identifier returned when the deferred function was called.

        Returns
        -------
        dict
            A dictionary containing the task_id, status, and result.
            If the task is not found or an error occurs, a dictionary with an error message is returned.

        Examples
        --------
        >>> result = caller.get_deferred_result("service.deferred", task_id)
        >>> print("Deferred Result:", result)
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

        Examples
        --------
        >>> caller.close()
        """
        asyncio.run_coroutine_threadsafe(self._nc.close(), self.loop).result()
        self.loop.call_soon_threadsafe(self.loop.stop)


##############################################################################
# Asynchronous Client (Caller) Implementation using Redis for KV Storage
##############################################################################
class AsyncEdaliteCaller:
    """
    Asynchronous client to call immediate or deferred functions registered on an
    EdaliteWorker via NATS messaging. Deferred task results are retrieved from Redis
    using an asynchronous Redis client.

    Parameters
    ----------
    nats_url : Union[str, List[str]], optional
        The URL or list of URLs of the NATS server(s). Default is "nats://localhost:4222".
    redis_url : str, optional
        The URL of the Redis server. Default is "redis://localhost:6379/1".
    debug : bool, optional
        If True, prints debug information. Default is False.

    Attributes
    ----------
    _nc : NATS
        The asynchronous NATS client instance.
    _redis : RedisAsync
        The asynchronous Redis client instance used to retrieve deferred task results.
    loop : asyncio.AbstractEventLoop
        The asyncio event loop in use.
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
        Asynchronously create an instance of AsyncEdaliteCaller and connect to
        the NATS server and Redis.

        Parameters
        ----------
        nats_url : Union[str, List[str]], optional
            The URL or list of URLs of the NATS server(s). Default is "nats://localhost:4222".
        redis_url : str, optional
            The URL of the Redis server. Default is "redis://localhost:6379/0".
        debug : bool, optional
            If True, prints debug information. Default is False.

        Returns
        -------
        AsyncEdaliteCaller
            An instance of AsyncEdaliteCaller with established connections.

        Examples
        --------
        >>> caller = await AsyncEdaliteCaller.connect(
        ...     nats_url="nats://localhost:4222",
        ...     redis_url="redis://localhost:6379/1",
        ...     debug=True
        ... )
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
        Asynchronously call an immediate execution function.

        Parameters
        ----------
        subject : str
            The subject to which the request is sent.
        data : Any
            The data to be sent. It will be converted to a string.
        timeout : float, optional
            The request timeout in seconds. Default is 30.0.

        Returns
        -------
        str
            The response returned by the worker.

        Raises
        ------
        RuntimeError
            If the function returns an error response.

        Examples
        --------
        >>> result = await caller.request("service.immediate", "Async Hello!")
        >>> print("Response:", result)
        """
        response = await self._nc.request(subject, str(data).encode(), timeout=timeout)
        result = response.data.decode()
        if result.startswith("Error:"):
            raise RuntimeError(result[6:].strip())
        return result

    async def delay(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """
        Asynchronously call a deferred execution function.

        This method sends a request to the worker and returns immediately with a
        generated task_id. The actual processing is done in the background by the worker.

        Parameters
        ----------
        subject : str
            The subject to which the request is sent.
        data : Any
            The data to be sent. It will be converted to a string.
        timeout : float, optional
            The request timeout in seconds. Default is 30.0.

        Returns
        -------
        str
            The task_id assigned by the worker.

        Raises
        ------
        RuntimeError
            If the function returns an error response.

        Examples
        --------
        >>> task_id = await caller.delay("service.deferred", "Async Deferred Hello!")
        >>> print("Task ID:", task_id)
        """
        response = await self._nc.request(subject, str(data).encode(), timeout=timeout)
        task_id = response.data.decode()
        if task_id.startswith("Error:"):
            raise RuntimeError(task_id[6:].strip())
        return task_id

    async def get_deferred_result(self, subject: str, task_id: str) -> dict:
        """
        Asynchronously retrieve the result of a deferred function from Redis.

        The Redis key format is: DEFERRED_TASKS_{subject}:{task_id}

        Parameters
        ----------
        subject : str
            The subject corresponding to the deferred function.
        task_id : str
            The task identifier returned when the deferred function was called.

        Returns
        -------
        dict
            A dictionary containing the task_id, status, and result, or an error message.

        Examples
        --------
        >>> result = await caller.get_deferred_result("service.deferred", task_id)
        >>> print("Deferred Result:", result)
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
        Asynchronously close the NATS and Redis connections.

        Examples
        --------
        >>> await caller.close()
        """
        await self._nc.close()
        await self._redis.close()

    async def subscribe(self, subject: str, callback: Callable):
        """
        Asynchronously subscribe to a broadcast message subject.

        The callback can be either an asynchronous or synchronous function.
        If it is synchronous, it will be executed in a separate thread.

        Parameters
        ----------
        subject : str
            The subject to subscribe to.
        callback : Callable
            The function to call when a message is received. The function should accept
            a single parameter (the NATSMessage).

        Examples
        --------
        >>> async def handle_msg(msg):
        ...     print("Broadcast received:", msg.data.decode())
        >>> await caller.subscribe("broadcast.subject", handle_msg)
        """
        await self._nc.subscribe(subject, cb=callback)
