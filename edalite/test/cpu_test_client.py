import asyncio
import time
from nats.aio.client import Client as NATS
import statistics


async def run_concurrent_test(nc, server_type, count, batch_size=10):
    """동시에 여러 요청을 보내는 테스트"""
    all_times = []
    total_start = time.time()

    print(f"\n=== {server_type} 서버 테스트 시작 ===")

    # batch_size 단위로 요청을 나누어 전송
    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        batch_count = batch_end - batch_start

        tasks = []
        start_time = time.time()

        # 배치 단위로 동시 요청
        for i in range(batch_start, batch_end):
            task = asyncio.create_task(
                nc.request("cpu.request", f"테스트 메시지 #{i+1}".encode(), timeout=30)
            )
            tasks.append(task)

        # 배치의 모든 응답 대기
        responses = await asyncio.gather(*tasks)
        batch_time = time.time() - start_time

        # 응답 시간 기록
        all_times.append(batch_time / batch_count)

        print(f"배치 {batch_start//batch_size + 1} 완료 ({batch_count}개)")
        print(f"배치 평균 처리 시간: {(batch_time/batch_count)*1000:.2f}ms")
        print(f"마지막 응답: {responses[-1].data.decode()}")

    total_time = time.time() - total_start

    # 통계 계산
    avg_time = statistics.mean(all_times)
    std_dev = statistics.stdev(all_times) if len(all_times) > 1 else 0

    print(f"\n=== {server_type} 서버 전체 통계 ===")
    print(f"총 처리 시간: {total_time:.2f}초")
    print(f"평균 처리 시간: {avg_time*1000:.2f}ms")
    print(f"표준 편차: {std_dev*1000:.2f}ms")
    print(f"처리량: {count/total_time:.2f} 요청/초")
    print("================\n")


async def main():
    nc = NATS()
    try:
        await nc.connect("nats://localhost:4222")
        print("NATS 서버에 연결되었습니다.")

        test_scenarios = [
            {"count": 100, "batch_size": 20},
            {"count": 200, "batch_size": 20},
            {"count": 500, "batch_size": 50},
        ]

        server_types = ["스레드", "비동기", "프로세스"]

        for server_type in server_types:
            print(f"\n=== {server_type} 서버 테스트 시작 ===")
            for scenario in test_scenarios:
                print(
                    f"\n=== {scenario['count']}개 메시지 테스트 (배치 크기: {scenario['batch_size']}) ==="
                )
                await run_concurrent_test(
                    nc, server_type, scenario["count"], scenario["batch_size"]
                )
                await asyncio.sleep(2)  # 서버 회복 시간

            if server_type != server_types[-1]:
                print(f"\n{server_type} 서버 테스트 완료. 10초 후 다음 테스트 시작...")
                await asyncio.sleep(10)

    except Exception as e:
        print(f"에러 발생: {str(e)}")
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
