import time
from newio import timeout, spawn, sleep

from .helper import run_it


@run_it
async def test_sleep_timeout():
    begin = time.monotonic()
    async with timeout(0.1) as is_timeout:
        await sleep(10)
    assert is_timeout
    cost = time.monotonic() - begin
    assert cost >= 0.1 and cost < 0.15


@run_it
async def test_task_timeout():
    task = await spawn(sleep(10))
    begin = time.monotonic()
    async with timeout(0.1) as is_timeout:
        await task.join()
    assert is_timeout
    cost = time.monotonic() - begin
    assert cost >= 0.1 and cost < 0.15
