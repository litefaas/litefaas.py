import asyncio
from litefaas_py.core import LitefaasCaller
import time


async def main():
    # 클라이언트 연결
    client = await LitefaasCaller.connect()

    try:
        print("서버에 연결됨")

        # 비동기 작업 10회 병렬 실행
        print("\n비동기 작업 10회 시작...")
        start_time = time.time()
        tasks = [
            client.request("async.task", f"테스트 메시지 {i}", timeout=30.0)
            for i in range(10)
        ]
        results = await asyncio.gather(*tasks)

        for i, result in enumerate(results):
            print(f"비동기 작업 {i+1}/10 완료: {result}")

        task_ids = []

        # 지연 작업 10회 실행
        print("\n지연 작업 10회 시작...")
        for i in range(10):
            task_id = await client.delay(
                "deferred.task", f"지연 메시지 {i}", timeout=30.0
            )
            print(f"지연 작업 {i+1}/10 완료: {task_id}")
            task_ids.append(task_id)

        time.sleep(1)

        # 지연 작업 결과 조회
        for task_id in task_ids:
            result = await client.get_deferred_result("deferred.task", task_id)
            print(f"지연 작업 결과: {result}")

        end_time = time.time()
        print(f"작업 총 소요시간: {end_time - start_time:.2f}초")

    except Exception as e:
        print(f"에러 발생: {e}")
        print("서버가 실행 중인지 확인하세요.")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
