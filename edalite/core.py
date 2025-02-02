#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Edalite Worker and Caller Implementation using NATS and JetStream KV Store
==========================================================================

This module provides a worker (server) and client (caller) implementation to
handle immediate and deferred function execution via a NATS messaging system.

Classes
-------
EdaliteWorker
    A synchronous worker that registers functions to be executed immediately
    or in a deferred (background) manner upon receiving messages.
EdaliteCaller
    A synchronous client to call immediate or deferred functions on the worker.
AsyncEdaliteCaller
    An asynchronous client to call immediate or deferred functions on the worker.

Usage Examples
--------------
Example using EdaliteWorker and EdaliteCaller (synchronous):

    from datetime import timedelta
    import time
    from edalite_module import EdaliteWorker, EdaliteCaller

    # Create and register worker functions
    worker = EdaliteWorker(nats_url="nats://localhost:4222", debug=True)

    @worker.task("service.immediate", queue_group="group1")
    def immediate_task(data):
        print(f"Immediate task received: {data}")
        return f"Processed immediate data: {data}"

    @worker.delayed_task("service.deferred", queue_group="group2", ttl=timedelta(seconds=60))
    def deferred_task(data):
        print(f"Deferred task processing: {data}")
        # Simulate processing delay
        import time
        time.sleep(2)
        return f"Processed deferred data: {data}"

    # Start the worker in a separate thread (blocks the main thread)
    # In production, you might run the worker as a separate service.
    threading.Thread(target=worker.start, daemon=True).start()

    # Allow some time for the worker to start and subscribe to subjects.
    time.sleep(1)

    # Create a synchronous client (caller)
    caller = EdaliteCaller(nats_url="nats://localhost:4222", debug=True).connect()

    # Call immediate function
    result_immediate = caller.request("service.immediate", "Hello Immediate!")
    print(f"Immediate response: {result_immediate}")

    # Call deferred function; this returns a task_id immediately.
    task_id = caller.delay("service.deferred", "Hello Deferred!")
    print(f"Deferred task_id: {task_id}")

    # Wait for the deferred task to complete and then fetch its result.
    time.sleep(3)
    deferred_result = caller.get_deferred_result("service.deferred", task_id)
    print(f"Deferred task result: {deferred_result}")

    # Clean up
    caller.close()


Example using AsyncEdaliteCaller (asynchronous):

    import asyncio
    from edalite_module import AsyncEdaliteCaller

    async def async_main():
        caller = await AsyncEdaliteCaller.connect(nats_url="nats://localhost:4222", debug=True)
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
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NATSMessage
from nats.js.api import KeyValueConfig
from typing import Callable, Any
from nats.js.kv import KeyValue


##############################################################################
# Synchronous Worker (Server) Implementation
##############################################################################
class EdaliteWorker:
    """
    A synchronous worker that registers functions for immediate and deferred
    execution based on messages received via NATS.

    Immediate functions are executed upon message receipt, while deferred
    functions are stored in a JetStream Key-Value store and processed in the
    background.

    Parameters
    ----------
    nats_url : str, optional
        The URL of the NATS server (default is "nats://localhost:4222").
    debug : bool, optional
        If True, prints debug information (default is False).

    Attributes
    ----------
    functions : dict
        A mapping of subjects to a list of tuples (function, queue_group) for
        immediate execution.
    deferred_functions : dict
        A mapping of subjects to a list of tuples (function, queue_group, ttl)
        for deferred execution.
    _nc : NATS or None
        The asynchronous NATS client instance.
    _js : JetStream or None
        The JetStream context from the NATS client.
    _kv_stores : dict
        A mapping of subjects to their respective Key-Value store instances.
    loop : asyncio.AbstractEventLoop or None
        The asyncio event loop running in a separate thread.
    """

    def __init__(self, nats_url: str = "nats://localhost:4222", debug: bool = False):
        self.nats_url = nats_url
        self.functions = (
            {}
        )  # {subject: [(func, queue_group), ...]} for immediate execution
        self.deferred_functions = (
            {}
        )  # {subject: [(func, queue_group, ttl), ...]} for deferred execution
        self._nc = None  # NATS client (asynchronous)
        self._js = None  # JetStream context
        self._kv_stores = {}  # {subject: KeyValue} per subject for deferred tasks
        self.debug = debug
        self.loop = None  # asyncio event loop running in background

    def task(self, subject: str, queue_group: str = None) -> Callable:
        """
        Decorator to register an immediate execution task.

        The decorated task will be executed synchronously (in a separate
        thread) upon receiving a message on the specified subject.

        Parameters
        ----------
        subject : str
            The subject (channel) on which the message will be received.
        queue_group : str, optional
            The queue group name for load balancing (default is None).

        Returns
        -------
        Callable
            The decorator task.

        Raises
        ------
        ValueError
            If the same queue_group is already registered for the subject.
        """

        def decorator(task: Callable):
            if subject not in self.functions:
                self.functions[subject] = []
            # Prevent duplicate registration for the same subject and queue group
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
        Decorator to register a delayed execution task.

        The message is acknowledged immediately with a generated task_id,
        while the actual processing is done in the background. The result
        is stored in a JetStream Key-Value (KV) store with the given TTL.

        Parameters
        ----------
        subject : str
            The subject (channel) on which the message will be received.
        queue_group : str, optional
            The queue group name for load balancing (default is None).
        ttl : timedelta, optional
            The time-to-live for the KV store entry (default is 1 day).

        Returns
        -------
        Callable
            The decorator task.

        Raises
        ------
        ValueError
            If the same queue_group is already registered for the subject.
        """

        def decorator(task: Callable):
            if subject not in self.deferred_functions:
                self.deferred_functions[subject] = []
            # Prevent duplicate registration for the same subject and queue group
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

        This method performs the following steps:
            1. Starts a background asyncio event loop in a separate thread.
            2. Initializes the NATS connection, JetStream context, and sets up subscriptions.
            3. Blocks the main thread indefinitely until a KeyboardInterrupt occurs.

        Examples
        --------
        >>> worker = EdaliteWorker(nats_url="nats://localhost:4222", debug=True)
        >>> @worker.task("my.subject")
        ... def my_immediate(data):
        ...     return f"Processed {data}"
        >>> worker.start()
        """
        # 1. Start a background asyncio event loop
        self.loop = asyncio.new_event_loop()
        loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        loop_thread.start()

        # 2. Initialize NATS, JetStream, and subscriptions in the event loop
        future = asyncio.run_coroutine_threadsafe(self._init_nats(), self.loop)
        try:
            future.result()  # Wait until initialization completes
        except Exception as e:
            if self.debug:
                print(f"Error during NATS initialization: {e}")
            return

        if self.debug:
            print("EdaliteWorker is running.")

        # 3. Keep the main thread alive until interrupted
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            if self.debug:
                print("Worker shutting down...")
            asyncio.run_coroutine_threadsafe(self._nc.close(), self.loop).result()
            self.loop.call_soon_threadsafe(self.loop.stop)

    def _run_loop(self):
        """
        Internal method to run the asyncio event loop in a separate thread.
        """
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _init_nats(self):
        """
        Initialize the NATS connection, JetStream context, create KV stores for
        deferred tasks, and set up subscriptions for both immediate and deferred
        functions.
        """
        self._nc = NATS()
        await self._nc.connect(
            self.nats_url,
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
            ping_interval=20,
            max_outstanding_pings=5,
        )

        print("Connected to NATS server")

        # Initialize JetStream context
        self._js = self._nc.jetstream()

        # Create a KV store for each subject with deferred functions
        for subject, handlers in self.deferred_functions.items():
            # Use a default TTL (1 day) or as specified per handler
            ttl = timedelta(days=1)
            kv_bucket = f"DEFERRED_TASKS_{subject.replace('.', '_')}"
            try:
                kv = await self._js.create_key_value(
                    config=KeyValueConfig(
                        bucket=kv_bucket,
                        ttl=int(ttl.total_seconds()),
                    )
                )
                self._kv_stores[subject] = kv
                if self.debug:
                    print(f"[KV] Bucket '{kv_bucket}' created with TTL={ttl}")
            except Exception as e:
                if self.debug:
                    print(f"[KV] Error creating bucket '{kv_bucket}': {e}")

        # Set up subscriptions for immediate execution functions
        for subject, handlers in self.functions.items():
            for task, queue_group in handlers:

                async def callback(msg: NATSMessage, f=task):
                    self._handle_message(msg, f)

                if queue_group:
                    await self._nc.subscribe(subject, cb=callback, queue=queue_group)
                else:
                    await self._nc.subscribe(subject, cb=callback)

        # Set up subscriptions for deferred execution functions
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
            print("  Delayed:", list(self.deferred_functions.keys()))

    def _handle_message(self, msg: NATSMessage, task: Callable):
        """
        Handle an immediate execution message by starting a new thread to
        process the message.

        Parameters
        ----------
        msg : NATSMessage
            The incoming message.
        task : Callable
            The user-defined task to process the message.
        """
        threading.Thread(
            target=self._process_message, args=(msg, task), daemon=True
        ).start()

    def _process_message(self, msg: NATSMessage, task: Callable):
        """
        Process an immediate execution message.

        This method decodes the message data, calls the registered task,
        and sends the result back as a response.

        Parameters
        ----------
        msg : NATSMessage
            The incoming message.
        task : Callable
            The task to be executed.
        """
        try:
            data = msg.data.decode()
            if self.debug:
                print(f"[Immediate] Received: {data}")
            # Execute the user-defined task
            result = task(data)
            if self.debug:
                print(f"[Immediate] Sending response: {result}")
            # Send the response asynchronously
            asyncio.run_coroutine_threadsafe(
                msg.respond(str(result).encode()), self.loop
            )
        except Exception as e:
            if self.debug:
                print(f"[Immediate] Error: {e}")
            asyncio.run_coroutine_threadsafe(
                msg.respond(f"Error: {str(e)}".encode()), self.loop
            )

    def _handle_deferred_message(
        self, msg: NATSMessage, task: Callable, ttl: timedelta
    ):
        """
        Handle a delayed execution message by starting a new thread to
        process the message in the background.

        Parameters
        ----------
        msg : NATSMessage
            The incoming message.
        task : Callable
            The task to be executed in a delayed manner.
        ttl : timedelta
            The time-to-live for the KV store entry.
        """
        threading.Thread(
            target=self._process_deferred_message, args=(msg, task, ttl), daemon=True
        ).start()

    def _process_deferred_message(
        self, msg: NATSMessage, task: Callable, ttl: timedelta
    ):
        """
        Process a delayed execution message.

        This method immediately responds with a generated task_id, then
        processes the task in the background and updates the task status in
        the KV store.

        Parameters
        ----------
        msg : NATSMessage
            The incoming message.
        task : Callable
            The task to process the delayed message.
        ttl : timedelta
            The TTL for the KV store entry.
        """
        # Generate a unique task_id and respond immediately
        task_id = str(uuid.uuid4())
        asyncio.run_coroutine_threadsafe(msg.respond(task_id.encode()), self.loop)

        # Process the deferred task in a separate thread
        def process_task():
            try:
                data = msg.data.decode()
                if self.debug:
                    print(f"[Deferred] Received: {data}, task_id={task_id}")
                subject = msg.subject
                self._publish_deferred_status(subject, task_id, "pending", None)
                result = task(data)
                self._publish_deferred_status(subject, task_id, "completed", result)
                if self.debug:
                    print(f"[Deferred] Task {task_id} completed with result: {result}")
            except Exception as e:
                self._publish_deferred_status(msg.subject, task_id, "error", str(e))
                if self.debug:
                    print(f"[Deferred] Task {task_id} failed: {e}")

        threading.Thread(target=process_task, daemon=True).start()

    def _publish_deferred_status(
        self, subject: str, task_id: str, status: str, result: Any
    ):
        """
        Publish the status of a deferred task by storing it in the KV store.

        Parameters
        ----------
        subject : str
            The subject corresponding to the deferred task.
        task_id : str
            The unique identifier of the task.
        status : str
            The current status of the task ('pending', 'completed', or 'error').
        result : Any
            The result of the task execution, or error information.
        """
        kv: KeyValue = self._kv_stores.get(subject)
        if not kv:
            return
        doc = {
            "task_id": task_id,
            "status": status,
            "result": result,
        }
        asyncio.run_coroutine_threadsafe(
            kv.put(task_id, json.dumps(doc).encode()), self.loop
        )


##############################################################################
# Synchronous Client (Caller) Implementation
##############################################################################
class EdaliteCaller:
    """
    A synchronous client to call immediate or deferred functions registered on
    an EdaliteWorker via NATS.

    This client manages its own background asyncio event loop to interact with
    the asynchronous NATS client.

    Parameters
    ----------
    nats_url : str, optional
        The URL of the NATS server (default is "nats://localhost:4222").
    debug : bool, optional
        If True, prints debug information (default is False).
    """

    def __init__(self, nats_url: str = "nats://localhost:4222", debug: bool = False):
        self.nats_url = nats_url
        self.debug = debug
        self._nc = None  # NATS client (asynchronous)
        self._js = None  # JetStream context
        self.loop = None  # Background asyncio event loop

    def connect(self) -> "EdaliteCaller":
        """
        Connect to the NATS server and initialize the JetStream context.

        This method starts a background asyncio event loop in a separate thread
        and initializes the asynchronous NATS client.

        Returns
        -------
        EdaliteCaller
            The instance itself after successful connection.

        Examples
        --------
        >>> caller = EdaliteCaller(nats_url="nats://localhost:4222", debug=True).connect()
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
        Asynchronously initialize the NATS client and JetStream context.
        """
        self._nc = NATS()
        await self._nc.connect(
            self.nats_url,
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
            ping_interval=20,
            max_outstanding_pings=5,
        )
        self._js = self._nc.jetstream()

    def request(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """
        Call an immediate execution function and wait synchronously for a response.

        Parameters
        ----------
        subject : str
            The subject to which the request is sent.
        data : Any
            The data to be sent (will be converted to string).
        timeout : float, optional
            The request timeout in seconds (default is 30.0).

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
            raise RuntimeError(f"함수 호출 실패 ({subject}): {e}")

    def delay(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """
        Call a deferred execution function.

        This method returns immediately with a task_id. The actual processing
        is done in the background by the worker.

        Parameters
        ----------
        subject : str
            The subject to which the request is sent.
        data : Any
            The data to be sent (will be converted to string).
        timeout : float, optional
            The request timeout in seconds (default is 30.0).

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
            raise RuntimeError(f"지연 함수 호출 실패({subject}): {e}")

    def get_deferred_result(self, subject: str, task_id: str) -> dict:
        """
        Retrieve the result of a deferred function from the KV store.

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
            If the task is not found or an error occurs, an error message is returned.

        Examples
        --------
        >>> result = caller.get_deferred_result("service.deferred", task_id)
        >>> print("Deferred Result:", result)
        """
        kv_bucket = f"DEFERRED_TASKS_{subject.replace('.', '_')}"
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._js.key_value(kv_bucket), self.loop
            )
            kv = future.result()
        except Exception as e:
            return {"error": f"KV 버킷 조회 실패: {e}"}
        try:
            future = asyncio.run_coroutine_threadsafe(kv.get(task_id), self.loop)
            entry = future.result()
            if not entry:
                return {
                    "error": f"task_id={task_id}에 대한 KV 데이터를 찾을 수 없습니다."
                }
            data = json.loads(entry.value)
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

    def subscribe(self, subject: str, callback: Callable):
        """
        Subscribe to a broadcast message subject.

        The provided callback is a synchronous function which will be executed
        in a separate thread upon message receipt.

        Parameters
        ----------
        subject : str
            The subject to subscribe to.
        callback : Callable
            The function to call when a message is received. The function should
            accept a single parameter (the NATSMessage).

        Examples
        --------
        >>> def handle_broadcast(msg):
        ...     print("Broadcast received:", msg.data.decode())
        >>>
        >>> caller.subscribe("broadcast.subject", handle_broadcast)
        """

        async def async_callback(msg: NATSMessage):
            threading.Thread(target=callback, args=(msg,), daemon=True).start()

        asyncio.run_coroutine_threadsafe(
            self._nc.subscribe(subject, cb=async_callback), self.loop
        )


##############################################################################
# Asynchronous Client (Caller) Implementation
##############################################################################
class AsyncEdaliteCaller:
    """
    An asynchronous client to call immediate or deferred functions registered on
    an EdaliteWorker via NATS.

    This client uses the currently running asyncio event loop.

    Parameters
    ----------
    nats_url : str, optional
        The URL of the NATS server (default is "nats://localhost:4222").
    debug : bool, optional
        If True, prints debug information (default is False).

    Class Methods
    -------------
    connect(nats_url: str, debug: bool) -> AsyncEdaliteCaller
        Asynchronously create an instance and connect to NATS.
    """

    def __init__(self, nats_url: str = "nats://localhost:4222", debug: bool = False):
        self.nats_url = nats_url
        self.debug = debug
        self._nc = None  # NATS client (asynchronous)
        self._js = None  # JetStream context
        # Use the currently running event loop or manage separately
        self.loop = asyncio.get_event_loop()

    @classmethod
    async def connect(
        cls, nats_url: str = "nats://localhost:4222", debug: bool = False
    ) -> "AsyncEdaliteCaller":
        """
        Asynchronously create an instance of AsyncEdaliteCaller and connect to the NATS server.

        Parameters
        ----------
        nats_url : str, optional
            The URL of the NATS server (default is "nats://localhost:4222").
        debug : bool, optional
            If True, prints debug information (default is False).

        Returns
        -------
        AsyncEdaliteCaller
            An instance of AsyncEdaliteCaller with an established connection.

        Examples
        --------
        >>> caller = await AsyncEdaliteCaller.connect(nats_url="nats://localhost:4222", debug=True)
        >>> result = await caller.request("service.immediate", "Hello!")
        """
        instance = cls(nats_url, debug)
        instance._nc = NATS()
        await instance._nc.connect(
            nats_url,
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
            ping_interval=20,
            max_outstanding_pings=5,
        )
        instance._js = instance._nc.jetstream()
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
            The data to be sent (will be converted to string).
        timeout : float, optional
            The request timeout in seconds (default is 30.0).

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

        This method returns immediately with a task_id. The actual processing
        is done in the background by the worker.

        Parameters
        ----------
        subject : str
            The subject to which the request is sent.
        data : Any
            The data to be sent (will be converted to string).
        timeout : float, optional
            The request timeout in seconds (default is 30.0).

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
        Asynchronously retrieve the result of a deferred function from the KV store.

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
        kv_bucket = f"DEFERRED_TASKS_{subject.replace('.', '_')}"
        try:
            kv = await self._js.key_value(kv_bucket)
        except Exception as e:
            return {"error": f"KV 버킷 조회 실패: {e}"}
        try:
            entry = await kv.get(task_id)
            if not entry:
                return {
                    "error": f"task_id={task_id}에 대한 KV 데이터를 찾을 수 없습니다."
                }
            data = json.loads(entry.value)
            return data
        except Exception as e:
            return {"error": str(e)}

    async def close(self):
        """
        Asynchronously close the NATS connection.

        Examples
        --------
        >>> await caller.close()
        """
        await self._nc.close()

    async def subscribe(self, subject: str, callback: Callable):
        """
        Subscribe to a broadcast message subject asynchronously.

        The callback can be either an asynchronous function or a synchronous
        function. In case of a synchronous callback, it will be executed in a
        separate thread.

        Parameters
        ----------
        subject : str
            The subject to subscribe to.
        callback : Callable
            The function to call when a message is received. It should accept a
            single parameter (the NATSMessage).

        Examples
        --------
        >>> async def handle_msg(msg):
        ...     print("Broadcast received:", msg.data.decode())
        >>>
        >>> await caller.subscribe("broadcast.subject", handle_msg)
        """
        await self._nc.subscribe(subject, cb=callback)
