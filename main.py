from edalite.core import EdaliteWorker
import random

worker = EdaliteWorker(debug=False)


@worker.task("example.immediate", queue_group="immediate_workers")
def echo(data):
    return f"Echo: {data}"


@worker.delayed_task("example.deferred", queue_group="deferred_workers")
def deferred_echo(data):
    # 시간이 오래 걸리는 작업이라 가정
    import time

    time.sleep(random.randint(1, 5))
    return f"Deferred Echo: {data}"


worker.start()
