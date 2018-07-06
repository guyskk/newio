import asyncio
import time
import logging
import pytest
from multiprocessing import cpu_count
from newio import spawn, run_in_process, run_in_thread, run_in_asyncio
from .helper import run_it

LOG = logging.getLogger(__name__)


async def thread_sleep_task(seconds):
    await run_in_thread(time.sleep, seconds)


async def process_sleep_task(seconds):
    await run_in_process(time.sleep, seconds)


async def asyncio_sleep_task(seconds):
    await run_in_asyncio(asyncio.sleep(seconds))


@pytest.mark.parametrize('sleep_task', [thread_sleep_task, asyncio_sleep_task])
@run_it
async def test_executor(sleep_task):
    cost = await run_sleep_task(sleep_task, seconds=0.2)
    assert 0.2 <= cost < 0.25


@pytest.mark.parametrize('sleep_task', [process_sleep_task])
@run_it
async def test_process_executor(sleep_task):
    # for single-core CPU
    num_tasks = min(3, cpu_count())
    # warm-up
    await run_sleep_task(sleep_task, seconds=0.02, num_tasks=num_tasks)
    cost = await run_sleep_task(sleep_task, seconds=0.2, num_tasks=num_tasks)
    assert 0.2 <= cost < 0.3


async def run_sleep_task(sleep_task, *, seconds, num_tasks=5):
    begin = time.monotonic()
    tasks = []
    for _ in range(num_tasks):
        tasks.append(await spawn(sleep_task(seconds)))
    for task in tasks:
        await task.join()
    return time.monotonic() - begin
