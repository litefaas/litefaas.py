from litefaas_py.core import LitefaasServer, run_server
import asyncio
import time

# FaaS 인스턴스 생성
faas = LitefaasServer()


@faas.function("async.task")
async def async_task(data: str):
    await asyncio.sleep(0.1)  # I/O 작업 시뮬레이션
    print(f"비동기 처리 완료: {data}")
    return f"비동기 처리 완료: {data}"


@faas.function("cpu.task")
def cpu_task(data: str):
    result = 0
    for i in range(5000):
        print(i)
    print(f"CPU 작업 완료 ({result % 10000}): {data}")
    return f"CPU 작업 완료 ({result % 10000}): {data}"


@faas.function("mixed.task")
def mixed_task(data: str):
    time.sleep(0.1)  # I/O 작업 시뮬레이션
    result = sum(i * i for i in range(10000))
    print(f"혼합 작업 완료 ({result % 10000}): {data}")
    return f"혼합 작업 완료 ({result % 10000}): {data}"


if __name__ == "__main__":
    # Windows에서 멀티프로세싱을 위한 설정
    from multiprocessing import freeze_support

    freeze_support()

    # 서버 시작
    run_server(faas)
