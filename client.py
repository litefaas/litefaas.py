#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test Client for EdaliteCaller and AsyncEdaliteCaller

이 스크립트는 EdaliteCaller (동기)와 AsyncEdaliteCaller (비동기)를 이용하여
즉시 작업과 지연 작업(Deferred)을 테스트합니다.
"""

import time
import asyncio
from edalite.core import EdaliteCaller, AsyncEdaliteCaller


def test_sync_client():
    print("=== Starting Synchronous Client Test ===")
    caller = EdaliteCaller(
        nats_url="nats://localhost:4222",
        redis_url="redis://localhost:6379/0",
        debug=True,
    ).connect()

    # 즉시 실행 테스트
    try:
        immediate_result = caller.request("service.task", "Hello Immediate!")
        print("Synchronous Immediate result:", immediate_result)
    except Exception as e:
        print("Error in synchronous immediate call:", e)

    # 지연 실행 테스트 (caller가 먼저 task_id를 생성하고 QUEUED 상태 저장)
    try:
        deferred_task_id = caller.delay("service.task", "Hello Deferred!")
        print("Synchronous Deferred task_id:", deferred_task_id)
        # 작업 처리 시간이 있으므로 잠시 대기 (예: 4초)
        # time.sleep(4)
        deferred_result = caller.get_deferred_result("service.task", deferred_task_id)
        print("Synchronous Deferred result:", deferred_result)
    except Exception as e:
        print("Error in synchronous deferred call:", e)

    caller.close()
    print("=== Synchronous Client Test Completed ===\n")


async def test_async_client():
    print("=== Starting Asynchronous Client Test ===")
    caller = await AsyncEdaliteCaller.connect(
        nats_url="nats://localhost:4222",
        redis_url="redis://localhost:6379/0",
        debug=True,
    )

    # # 즉시 실행 테스트
    # try:
    #     for i in range(10):
    #         immediate_result = await caller.request(
    #             "service.task", f"Async Hello Immediate {i}!"
    #         )
    #         print(f"Async Immediate result {i}:", immediate_result)
    # except Exception as e:
    #     print("Error in async immediate call:", e)

    # 지연 실행 테스트 (caller가 먼저 task_id를 생성하고 QUEUED 상태 저장)
    try:
        deferred_task_id_list = []
        for i in range(10):
            deferred_task_id = await caller.delay(
                "service.task", f"Async Hello Deferred {i}!"
            )
            print(f"Async Deferred task_id {i}:", deferred_task_id)
            deferred_task_id_list.append(deferred_task_id)

        # 작업 처리 시간이 있으므로 잠시 대기 (예: 4초)
        await asyncio.sleep(4)
        for deferred_task_id in deferred_task_id_list:
            deferred_result = await caller.get_deferred_result(
                "service.task", deferred_task_id
            )
            print(f"Async Deferred result {i}:", deferred_result)

    except Exception as e:
        print("Error in async deferred call:", e)

    await caller.close()
    print("=== Asynchronous Client Test Completed ===\n")


def main():
    # 동기 클라이언트 테스트 실행
    # test_sync_client()

    # 비동기 클라이언트 테스트 실행
    asyncio.run(test_async_client())


if __name__ == "__main__":
    main()
