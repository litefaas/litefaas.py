#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test Worker for EdaliteWorker

이 스크립트는 EdaliteWorker를 생성하고, 
예제 작업을 등록한 후 NATS 메시지를 수신하며 동작하는 워커를 시작합니다.
"""

import time
import threading
from edalite.core import EdaliteWorker


# EdaliteWorker 생성 (여기서는 동기 워커 사용, ThreadPoolExecutor로 2개의 스레드 사용)
worker = EdaliteWorker(
    nats_url="nats://localhost:4222",
    redis_url="redis://localhost:6379/0",
    max_process=10,
    max_thread=1,
    debug=True,
)


# @task 데코레이터로 작업 등록 (즉시/지연 모두 처리)
@worker.task("service.task", queue_group="group1")
def my_task(data):
    print(f"Worker: Received task data: {data}")
    # 처리 시간을 시뮬레이션 (예: 2초 지연)
    time.sleep(2)
    result = f"Worker: Processed {data}"
    print(f"Worker: Completed task with result: {result}")
    return result


print("Starting worker...")
# 워커는 내부적으로 무한 루프를 돌며 메시지를 수신하므로,
# 별도 스레드 또는 프로세스로 실행하여 종료되지 않도록 합니다.


if __name__ == "__main__":
    from multiprocessing import freeze_support

    freeze_support()
    worker.start()
