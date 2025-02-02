import asyncio
import uuid
import json
from datetime import timedelta
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NATSMessage
from nats.js.api import KeyValueConfig
from typing import Callable, Any
from nats.js.kv import KeyValue


class LitefaasWorker:
    def __init__(self, nats_url: str = "nats://localhost:4222", debug: bool = False):
        self.nats_url = nats_url
        self.functions = {}  # {subject: [(func, queue_group), ...]}
        self.deferred_functions = {}  # {subject: [(func, queue_group, ttl), ...]}
        self._nc: NATS = None
        self._js = None  # JetStream 객체
        self._kv_stores = {}  # {subject: KeyValue} - subject별 Key-Value 스토어
        self.debug = debug

    def func(self, subject: str, queue_group: str = None):
        """일반 함수 데코레이터 (즉시 실행)"""

        def decorator(func: Callable):
            if subject not in self.functions:
                self.functions[subject] = []

            # subject+queue_group 조합 중복 검사
            for existing_func, existing_queue in self.functions[subject]:
                if existing_queue == queue_group:
                    raise ValueError(
                        f"Queue group '{queue_group}' is already registered for subject '{subject}'"
                    )

            self.functions[subject].append((func, queue_group))
            return func

        return decorator

    def deferred(
        self, subject: str, queue_group: str = None, ttl: timedelta = timedelta(days=1)
    ):
        """
        메시지를 큐(여기서는 KV 스토어)에 쌓고, 즉시 task_id를 응답.
        실제 처리는 백그라운드에서 진행.
        TTL(만료기간)은 기본적으로 1일로 설정되며,
        데코레이터에서 변경 가능.
        """

        def decorator(func: Callable):
            if subject not in self.deferred_functions:
                self.deferred_functions[subject] = []

            # subject+queue_group 조합 중복 검사
            for existing_func, existing_queue, _ in self.deferred_functions.get(
                subject, []
            ):
                if existing_queue == queue_group:
                    raise ValueError(
                        f"Queue group '{queue_group}' is already registered for subject '{subject}'"
                    )

            self.deferred_functions[subject].append((func, queue_group, ttl))
            return func

        return decorator

    async def _handle_message(self, msg: NATSMessage, func: Callable):
        """일반 함수 즉시 실행 처리"""
        asyncio.ensure_future(self._process_message(msg, func))

    async def _process_message(self, msg: NATSMessage, func: Callable):
        try:
            data = msg.data.decode()
            if self.debug:
                print(f"[Immediate] Request received: {data}")

            # 동기/비동기 함수 모두 처리
            if asyncio.iscoroutinefunction(func):
                result = await func(data)
            else:
                result = func(data)

            if self.debug:
                print(f"[Immediate] Sending response: {result}")
            await msg.respond(str(result).encode())
        except Exception as e:
            if self.debug:
                print(f"[Immediate] Error occurred: {str(e)}")
            await msg.respond(f"Error: {str(e)}".encode())

    async def _handle_deferred_message(
        self, msg: NATSMessage, func: Callable, ttl: timedelta
    ):
        """지연 실행(큐잉) 방식 처리"""
        asyncio.ensure_future(self._process_deferred_message(msg, func, ttl))

    async def _process_deferred_message(
        self, msg: NATSMessage, func: Callable, ttl: timedelta
    ):
        # 1) 우선 task_id 생성 후 바로 응답
        task_id = str(uuid.uuid4())
        await msg.respond(task_id.encode())

        # 2) 백그라운드에서 실제 작업 처리
        async def process_task():
            try:
                data = msg.data.decode()
                if self.debug:
                    print(f"[Deferred] Request received: {data}, task_id={task_id}")

                # KV에 'pending' 상태 저장
                subject = msg.subject
                await self._publish_deferred_status(subject, task_id, "pending", None)

                # 실제 함수 호출
                if asyncio.iscoroutinefunction(func):
                    result = await func(data)
                else:
                    result = func(data)

                # KV에 'completed' 상태 저장
                await self._publish_deferred_status(
                    subject, task_id, "completed", result
                )

                if self.debug:
                    print(f"[Deferred] Task {task_id} completed with result: {result}")

            except Exception as e:
                # KV에 'error' 상태 저장
                await self._publish_deferred_status(
                    msg.subject, task_id, "error", str(e)
                )

                if self.debug:
                    print(f"[Deferred] Task {task_id} failed: {str(e)}")

        # 3) 백그라운드 태스크로 실행
        asyncio.create_task(process_task())

    async def _publish_deferred_status(
        self, subject: str, task_id: str, status: str, result: Any
    ):
        """
        task_id 기준으로 상태 정보를 KV 스토어에 저장.
        """
        kv: KeyValue = self._kv_stores.get(subject)
        if not kv:
            return

        doc = {
            "task_id": task_id,
            "status": status,
            "result": result,
        }
        # key는 task_id를 그대로 사용
        await kv.put(task_id, json.dumps(doc).encode())

    async def start(self):
        """서버 시작"""
        if not self.functions and not self.deferred_functions:
            raise ValueError("No registered functions.")

        self._nc = NATS()
        try:
            await self._nc.connect(
                self.nats_url,
                reconnect_time_wait=2,
                max_reconnect_attempts=-1,
                ping_interval=20,
                max_outstanding_pings=5,
            )
            print("Connected to NATS server")

            # JetStream 초기화
            self._js = self._nc.jetstream()

            # subject별로 KV 스토어 생성
            for subject, handlers in self.deferred_functions.items():
                ttl = timedelta(days=1)  # 기본 TTL 설정
                kv_bucket = f"DEFERRED_TASKS_{subject.replace('.', '_')}"
                try:
                    kv = await self._js.create_key_value(
                        config=KeyValueConfig(
                            bucket=kv_bucket,
                            ttl=int(ttl.total_seconds()),  # TTL을 초 단위로 변환
                        )
                    )
                    self._kv_stores[subject] = kv
                    if self.debug:
                        print(f"[KV] '{kv_bucket}' 버킷을 생성했습니다. TTL={ttl}")
                except Exception as e:
                    if self.debug:
                        print(
                            f"[KV] 버킷 생성 중 예외 발생(이미 존재할 수 있음): {str(e)}"
                        )

            # 기존 Stream(Immediate) 관련 구독 설정
            for subject, handlers in self.functions.items():
                for func, queue_group in handlers:

                    def create_handler(f):
                        async def message_handler(msg):
                            await self._handle_message(msg, f)

                        return message_handler

                    if queue_group:
                        await self._nc.subscribe(
                            subject, cb=create_handler(func), queue=queue_group
                        )
                    else:
                        await self._nc.subscribe(subject, cb=create_handler(func))

            # 지연 실행 함수 구독 설정
            for subject, handlers in self.deferred_functions.items():
                for func, queue_group, ttl in handlers:

                    def create_deferred_handler(f, used_ttl):
                        async def message_handler(msg):
                            await self._handle_deferred_message(msg, f, used_ttl)

                        return message_handler

                    if queue_group:
                        await self._nc.subscribe(
                            subject,
                            cb=create_deferred_handler(func, ttl),
                            queue=queue_group,
                        )
                    else:
                        await self._nc.subscribe(
                            subject, cb=create_deferred_handler(func, ttl)
                        )

            if self.debug:
                print(
                    f"Server started - Immediate subjects: {list(self.functions.keys())}, "
                    f"Deferred subjects: {list(self.deferred_functions.keys())}"
                )

            await asyncio.Event().wait()

        except Exception as e:
            if self.debug:
                print(f"Server start failed: {str(e)}")


class LitefaasCaller:
    def __init__(self, nc: NATS):
        self._nc = nc
        self._js = None

    @classmethod
    async def connect(cls, nats_url: str = "nats://localhost:4222") -> "LitefaasCaller":
        """클라이언트 인스턴스 생성"""
        nc = NATS()
        await nc.connect(nats_url)
        instance = cls(nc)
        # JetStream 초기화
        instance._js = nc.jetstream()
        return instance

    async def request(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """즉시 실행 함수 호출"""
        try:
            response = await self._nc.request(
                subject, str(data).encode(), timeout=timeout
            )
            result = response.data.decode()
            if result.startswith("Error:"):
                raise RuntimeError(result[6:].strip())
            return result
        except Exception as e:
            raise RuntimeError(f"함수 호출 실패 ({subject}): {str(e)}")

    async def delay(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """
        지연 실행 함수를 호출. 즉시 task_id를 반환하고,
        실제 작업은 백그라운드에서 진행.
        """
        try:
            response = await self._nc.request(
                subject, str(data).encode(), timeout=timeout
            )
            task_id = response.data.decode()
            if task_id.startswith("Error:"):
                raise RuntimeError(task_id[6:].strip())
            return task_id
        except Exception as e:
            raise RuntimeError(f"지연 함수 호출 실패({subject}): {str(e)}")

    async def get_deferred_result(self, subject: str, task_id: str) -> dict:
        """
        KV 저장소에 저장된 deferred 결과 조회 메서드.
        subject별로 다른 KV 버킷이 생성되므로 subject가 필요.
        """
        if not self._js:
            raise RuntimeError(
                "JetStream이 초기화되지 않았습니다. connect() 메서드 호출 여부를 확인해주세요."
            )

        # KV 버킷 이름 (서버 쪽에서 subject에 따라 만든 것과 동일 규칙)
        kv_bucket = f"DEFERRED_TASKS_{subject.replace('.', '_')}"

        try:
            kv = await self._js.key_value(kv_bucket)
        except Exception as e:
            return {"error": f"KV 버킷 조회 실패: {str(e)}"}

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
        """연결 종료"""
        await self._nc.close()

    async def subscribe(self, subject: str, callback):
        """브로드캐스트 메시지 구독"""
        await self._nc.subscribe(subject, cb=callback)


def run_worker(faas_instance: LitefaasWorker):
    """Run server"""
    try:
        asyncio.run(faas_instance.start())
    except KeyboardInterrupt:
        print("Server is shutting down.")
