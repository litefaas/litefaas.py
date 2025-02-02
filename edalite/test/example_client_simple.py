import asyncio
from edalite.core import LitefaasClient

async def main():
    # 클라이언트 연결
    client = await LitefaasClient.connect()

    try:
        print("서버에 연결됨")

        # 단일 함수 호출
        print("비동기 작업 시작...")
        result = await client.call("async.task", "테스트 메시지", timeout=30.0)
        print(f"비동기 작업 결과: {result}")

        print("\nCPU 작업 시작...")
        result = await client.call("cpu.task", "CPU 테스트", timeout=30.0)
        print(f"CPU 작업 결과: {result}")

        # 여러 함수 동시 호출
        print("\n동시 호출 테스트:")
        tasks = []
        for i in range(3):
            task = client.call("mixed.task", f"병렬 테스트 {i+1}")
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        for i, result in enumerate(results, 1):
            print(f"병렬 테스트 {i} 결과: {result}")

    except Exception as e:
        print(f"에러 발생: {e}")
        print("서버가 실행 중인지 확인하세요.")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
