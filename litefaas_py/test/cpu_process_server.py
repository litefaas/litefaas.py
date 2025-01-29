import asyncio
from nats.aio.client import Client as NATS
from multiprocessing import Pool, cpu_count, freeze_support
import time
from collections import deque
from statistics import mean

# 처리 시간 추적을 위한 큐
processing_times = deque(maxlen=100)


def cpu_intensive_task(data: str) -> str:
    """CPU 집약적인 작업 (프로세스 풀용)"""
    start = time.time()

    # CPU를 많이 사용하는 계산
    result = 0
    for i in range(5000000):  # 500만 번 반복
        result += i * i
        # 더 복잡한 계산 추가
        if i % 2 == 0:
            result = result * 3
        else:
            result = result // 2

    duration = time.time() - start
    return f"프로세스 처리 결과: {result % 10000}, 계산 시간: {duration:.3f}초, 입력: {data}"


def print_stats(prefix="프로세스"):
    if not processing_times:
        return

    times = list(processing_times)
    avg_time = mean(times) * 1000

    print(f"\n=== {prefix} 처리 통계 ===")
    print(f"처리된 메시지 수: {len(times)}")
    print(f"평균 처리 시간: {avg_time:.2f}ms")
    if len(times) >= 100:
        print(f"최근 100개 평균: {avg_time:.2f}ms")
    if len(times) >= 10:
        print(f"최근 10개 평균: {mean(list(times)[-10:]) * 1000:.2f}ms")
    print(f"마지막 처리 시간: {times[-1] * 1000:.2f}ms")
    print("================\n")


async def run_server():
    # 프로세스 풀 생성 (CPU 코어 수만큼)
    process_pool = Pool(processes=cpu_count())

    # NATS 클라이언트 생성
    nc = NATS()

    try:
        await nc.connect("nats://localhost:4222")
        print(f"프로세스 서버: NATS 서버에 연결됨 (워커 수: {cpu_count()})")

        async def message_handler(msg):
            try:
                start_time = time.time()

                data = msg.data.decode()
                print(f"프로세스 서버 요청 받음: {data}")

                # CPU 집약적 작업을 프로세스 풀에서 실행
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(
                    None, process_pool.apply, cpu_intensive_task, (data,)
                )

                await msg.respond(response.encode())

                processing_time = time.time() - start_time
                processing_times.append(processing_time)

                if (
                    len(processing_times) in [1, 10, 100]
                    or len(processing_times) % 100 == 0
                ):
                    print_stats()

            except Exception as e:
                print(f"에러 발생: {str(e)}")

        # 구독자 설정
        for _ in range(cpu_count()):  # CPU 코어 수만큼 구독자 생성
            await nc.subscribe(
                "cpu.request", cb=message_handler, queue="process_workers"
            )

        print("프로세스 서버: 구독 시작")

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            print("프로세스 서버가 종료됩니다.")

    except Exception as e:
        print(f"프로세스 서버 에러: {str(e)}")
    finally:
        process_pool.close()
        process_pool.join()
        await nc.close()


def main():
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        print("프로세스 서버가 종료됩니다.")


if __name__ == "__main__":
    # Windows에서 멀티프로세싱을 위한 필수 호출
    freeze_support()
    main()
