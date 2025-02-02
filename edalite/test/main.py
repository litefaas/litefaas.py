from edalite.core import EdaliteWorker
import random


worker = EdaliteWorker(
    nats_url="nats://localhost:4222",
    redis_url="redis://localhost:6379/0",
    debug=True,
    max_process=2,
    max_thread=1,
)


@worker.task("example.immediate", queue_group="immediate_workers")
def echo(data):
    return f"Echo: {data}"


@worker.delayed_task("example.deferred", queue_group="deferred_workers")
def deferred_echo(data):
    # 시간이 오래 걸리는 작업이라 가정
    import time

    time.sleep(random.randint(1, 5))
    return f"Deferred Echo: {data}"


if __name__ == "__main__":
    from multiprocessing import freeze_support

    freeze_support()

    worker.start()
