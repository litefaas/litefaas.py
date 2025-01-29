import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout
from concurrent.futures import ThreadPoolExecutor
import functools
import time
from collections import deque
from statistics import mean

# 메시지 처리를 위한 스레드 풀 생성
thread_pool = ThreadPoolExecutor(max_workers=4)

# 처리 시간 추적을 위한 큐
processing_times = deque(maxlen=100)


def cpu_intensive_task(data: str) -> str:
    """CPU 집약적인 작업"""
    start = time.time()

    # CPU를 많이 사용하는 계산
    result = 0
    for i in range(1000000):  # 백만 번 반복
        result += i * i
        # 더 복잡한 계산 추가
        if i % 2 == 0:
            result = result * 2
        else:
            result = result // 3

    duration = time.time() - start
    return (
        f"스레드 처리 결과: {result % 10000}, 계산 시간: {duration:.3f}초, 입력: {data}"
    )


def print_stats(prefix="스레드"):
    """현재 처리 통계 출력"""
    if not processing_times:
        return

    times = list(processing_times)
    avg_time = mean(times) * 1000  # ms로 변환

    print(f"\n=== {prefix} 처리 통계 ===")
    print(f"처리된 메시지 수: {len(times)}")
    print(f"평균 처리 시간: {avg_time:.2f}ms")

    if len(times) >= 100:
        print(f"최근 100개 평균: {avg_time:.2f}ms")
    if len(times) >= 10:
        print(f"최근 10개 평균: {mean(list(times)[-10:]) * 1000:.2f}ms")
    print(f"마지막 처리 시간: {times[-1] * 1000:.2f}ms")
    print("================\n")


async def message_handler(msg):
    try:
        start_time = time.time()

        # 메시지 데이터 디코딩
        data = msg.data.decode()
        print(f"스레드 서버 요청 받음: {data}")

        # CPU 집약적 작업을 스레드 풀에서 실행
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(thread_pool, cpu_intensive_task, data)

        # 응답 전송
        await msg.respond(response.encode())

        # 처리 시간 기록
        processing_time = time.time() - start_time
        processing_times.append(processing_time)

        # 통계 출력 (1, 10, 100개 단위로)
        if len(processing_times) in [1, 10, 100] or len(processing_times) % 100 == 0:
            print_stats()

    except Exception as e:
        print(f"에러 발생: {str(e)}")


async def main():
    # NATS 클라이언트 생성
    nc = NATS()

    try:
        # NATS 서버에 연결 (재연결 옵션 추가)
        await nc.connect(
            "nats://localhost:4222",  # docker-compose 네트워크에서의 서비스 이름 사용
            max_reconnect_attempts=-1,  # 무제한 재연결 시도
            reconnect_time_wait=2,  # 재연결 대기 시간
            ping_interval=20,  # ping 간격
            max_outstanding_pings=5,  # 최대 미응답 ping 수
        )
        print("스레드 서버: NATS 서버에 연결됨")

        # 여러 개의 구독자 설정 (필요한 경우)
        subscription_tasks = []
        for _ in range(4):  # 스레드 풀 크기와 동일하게 설정
            sub = await nc.subscribe(
                "cpu.request",
                cb=message_handler,
                queue="thread_workers",  # 로드 밸런싱을 위한 큐 그룹
            )
            subscription_tasks.append(sub)

        print("스레드 서버: 구독 시작")

        # 종료 시그널 대기
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            print("스레드 서버가 종료됩니다.")

    except Exception as e:
        print(f"스레드 서버 에러: {str(e)}")
    finally:
        # 스레드 풀 정리
        thread_pool.shutdown(wait=True)
        # 연결 종료
        await nc.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("스레드 서버가 종료됩니다.")
