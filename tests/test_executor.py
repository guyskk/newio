import newio as nio
import time

from .helper import run_it


@run_it
async def test_run_in_thread():
    async def sleep_task(seconds):
        await nio.run_in_thread(time.sleep, 0.1)

    # warming up thread pool
    tasks = []
    for _ in range(5):
        tasks.append(await nio.spawn(sleep_task(0.01)))
    for task in tasks:
        await task.join()

    begin = time.monotonic()
    tasks = []
    for _ in range(5):
        tasks.append(await nio.spawn(sleep_task(0.1)))
    for task in tasks:
        await task.join()
    cost = time.monotonic() - begin
    assert 0.1 <= cost < 0.15


@run_it
async def test_run_in_process():
    async def sleep_task(seconds):
        await nio.run_in_process(time.sleep, seconds)

    # warming up process pool
    tasks = []
    for _ in range(2):
        tasks.append(await nio.spawn(sleep_task(0.01)))
    for task in tasks:
        await task.join()

    begin = time.monotonic()
    tasks = []
    for _ in range(2):
        tasks.append(await nio.spawn(sleep_task(0.1)))
    for task in tasks:
        await task.join()
    cost = time.monotonic() - begin
    assert 0.1 <= cost < 0.15
