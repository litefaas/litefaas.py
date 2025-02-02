import multiprocessing
import os
from edalite.core import LitefaasServer, run_server
import asyncio

# 서버 인스턴스
faas = LitefaasServer()


@faas.function("async.task", queue_group="workers")
async def async_task(data: str):
    await asyncio.sleep(0.1)  # I/O 작업 시뮬레이션
    print(f"[PID {os.getpid()}] 비동기 처리 완료: {data}")
    return f"[PID {os.getpid()}] 비동기 처리 완료: {data}"


def worker_main():
    # 멀티프로세스 각각에서 서버 실행
    run_server(faas)


if __name__ == "__main__":
    # 워커 개수 예: CPU 코어 수만큼
    num_workers = 4

    processes = []
    for _ in range(num_workers):
        p = multiprocessing.Process(target=worker_main)
        p.start()
        processes.append(p)

    # 모든 워커가 꺼질 때까지 대기
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
