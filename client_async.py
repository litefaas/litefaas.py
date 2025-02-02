import asyncio
from edalite.core import AsyncEdaliteCaller


async def main():
    caller = await AsyncEdaliteCaller.connect(debug=True)
    # result = await caller.request("example.immediate", "Hello from async!")
    # print("Async immediate result:", result)

    # 병렬로 5번 delay 실행
    tasks = [
        caller.delay("example.deferred", f"Async deferred hello {i}!")
        for i in range(15)
    ]
    task_ids = await asyncio.gather(*tasks)

    # task_id 출력
    for i, task_id in enumerate(task_ids):
        print(f"Task {i} ID:", task_id)

    await asyncio.sleep(2)

    for i, task_id in enumerate(task_ids):
        result = await caller.get_deferred_result("example.deferred", task_id)
        print(f"Task {i} Result:", result)

    await caller.close()


asyncio.run(main())
