import asyncio
import time
import pytest
from newio import spawn, run_in_process, run_in_thread, run_in_asyncio
from .helper import run_it


async def thread_sleep_task(seconds):
    await run_in_thread(time.sleep, seconds)


async def process_sleep_task(seconds):
    await run_in_process(time.sleep, seconds)


async def asyncio_sleep_task(seconds):
    await run_in_asyncio(asyncio.sleep(seconds))


@pytest.mark.parametrize('sleep_task', [
    thread_sleep_task,
    process_sleep_task,
    asyncio_sleep_task,
])
@run_it
async def test_executor(sleep_task):
    # warming up
    tasks = []
    for _ in range(3):
        tasks.append(await spawn(sleep_task(0.01)))
    for task in tasks:
        await task.join()
    # test
    begin = time.monotonic()
    tasks = []
    for _ in range(3):
        tasks.append(await spawn(sleep_task(0.2)))
    for task in tasks:
        await task.join()
    cost = time.monotonic() - begin
    assert 0.2 <= cost < 0.3
