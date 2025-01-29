import asyncio
from nats.aio.client import Client as NATS
from typing import Callable, Any


class LitefaasServer:
    def __init__(self, nats_url: str = "nats://localhost:4222"):
        self.nats_url = nats_url
        self.functions = {}
        self._nc = None

    def function(self, subject: str):
        """함수 데코레이터"""

        def decorator(func: Callable):
            self.functions[subject] = func
            return func

        return decorator

    async def _handle_message(self, msg, func: Callable):
        try:
            data = msg.data.decode()
            print(f"요청 받음: {data}")  # 디버깅용 로그 추가

            # 함수가 코루틴인지 확인하고 적절히 처리
            if asyncio.iscoroutinefunction(func):
                result = await func(data)
            else:
                result = func(data)

            print(f"응답 전송: {result}")  # 디버깅용 로그 추가
            await msg.respond(str(result).encode())

        except Exception as e:
            print(f"에러 발생: {str(e)}")  # 디버깅용 로그 추가
            await msg.respond(f"Error: {str(e)}".encode())

    async def start(self):
        """서버 시작"""
        if not self.functions:
            raise ValueError("등록된 함수가 없습니다.")

        self._nc = NATS()
        try:
            await self._nc.connect(
                self.nats_url,
                reconnect_time_wait=2,
                max_reconnect_attempts=-1,
                ping_interval=20,
                max_outstanding_pings=5,
            )
            print("NATS 서버에 연결됨")

            # 각 함수에 대한 구독 설정
            for subject, func in self.functions.items():
                # 클로저 문제를 해결하기 위해 함수를 생성하는 함수 사용
                def create_handler(f):
                    async def message_handler(msg):
                        await self._handle_message(msg, f)

                    return message_handler

                await self._nc.subscribe(subject, cb=create_handler(func))

            print(f"서버 시작됨 - 등록된 함수: {list(self.functions.keys())}")
            await asyncio.Event().wait()
        except Exception as e:
            print(f"서버 시작 실패: {str(e)}")


class LitefaasClient:
    def __init__(self, nc: NATS):
        self._nc = nc

    @classmethod
    async def connect(cls, nats_url: str = "nats://localhost:4222") -> "LitefaasClient":
        """클라이언트 인스턴스 생성"""
        nc = NATS()
        await nc.connect(nats_url)
        return cls(nc)

    async def call(self, subject: str, data: Any, timeout: float = 30.0) -> str:
        """함수 호출"""
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

    async def close(self):
        """연결 종료"""
        await self._nc.close()

    async def subscribe(self, subject: str, callback):
        """브로드캐스트 메시지 구독"""
        await self._nc.subscribe(subject, cb=callback)


def run_server(faas_instance: LitefaasServer):
    """서버 실행"""
    try:
        asyncio.run(faas_instance.start())
    except KeyboardInterrupt:
        print("서버가 종료됩니다.")
