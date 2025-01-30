import time
from litefaas_py.core import LitefaasCaller


def main():
    # 클라이언트 연결
    client = LitefaasCaller.connect()

    try:
        print("서버에 연결됨")

        # 동기 작업 10회 실행
        print("\n동기 작업 10회 시작...")
        start_time = time.time()
        for i in range(10):
            result = client.request_sync(
                "sync.task", f"테스트 메시지 {i}", timeout=30.0
            )
            print(f"동기 작업 {i+1}/10 완료: {result}")

        # 지연 작업 10회 실행
        print("\n지연 작업 10회 시작...")
        task_ids = []
        for i in range(10):
            task_id = client.delay_sync(
                "deferred.task", f"지연 메시지 {i}", timeout=30.0
            )
            task_ids.append(task_id)

        time.sleep(2)

        # 지연 작업 결과 조회
        for task_id in task_ids:
            result = client.get_deferred_result_sync("deferred.task", task_id)
            print(f"지연 작업 결과: {result}")

        end_time = time.time()
        print(f"작업 총 소요시간: {end_time - start_time:.2f}초")

    except Exception as e:
        print(f"에러 발생: {e}")
        print("서버가 실행 중인지 확인하세요.")
    finally:
        client.close_sync()


if __name__ == "__main__":
    main()
