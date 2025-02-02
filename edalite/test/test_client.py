import asyncio
import time
from nats.aio.client import Client as NATS


async def main():
    # NATS 클라이언트 생성
    nc = NATS()

    try:
        # NATS 서버에 연결
        await nc.connect("nats://localhost:4222")
        print("NATS 서버에 연결되었습니다.")

        # 테스트할 메시지 수
        message_counts = [1, 10, 100]

        for count in message_counts:
            print(f"\n=== {count}개 메시지 전송 테스트 시작 ===")
            start_time = time.time()

            # 동시에 여러 메시지 전송
            tasks = []
            for i in range(count):
                task = asyncio.create_task(
                    nc.request(
                        "service.request", f"테스트 메시지 #{i+1}".encode(), timeout=5
                    )
                )
                tasks.append(task)

            # 모든 응답 대기
            await asyncio.gather(*tasks)

            total_time = time.time() - start_time
            print(f"{count}개 메시지 처리 완료:")
            print(f"총 소요 시간: {total_time:.2f}초")
            print(f"메시지당 평균 시간: {(total_time/count)*1000:.2f}ms")
            print("================================")

    except Exception as e:
        print(f"에러 발생: {str(e)}")
    finally:
        # 연결 종료
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
