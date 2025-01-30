from litefaas_py.core import LitefaasWorker, run_worker
import asyncio

# FaaS 인스턴스 생성
faas = LitefaasWorker(debug=True)


@faas.func("async.task", queue_group="workers")
async def async_task(data: str):
    await asyncio.sleep(1)  # I/O 작업 시뮬레이션
    print(f"async_task 처리 완료: {data}")
    return f"async_task 처리 완료: {data}"


@faas.deferred("deferred.task", queue_group="deferred_workers")
async def deferred_task(data: str):

    await asyncio.sleep(0.1)

    for i in range(2000):
        print(f"deferred_task 처리 중: {i}")
    print(f"deferred_task 처리 완료: {data}")
    return f"deferred_task 처리 완료: {data}"


if __name__ == "__main__":
    # 서버 시작
    run_worker(faas)
