import asyncio
import time
import logging
import pytest
from multiprocessing import cpu_count
from newio import (
    spawn,
    run_in_process,
    run_in_thread,
    run_in_asyncio,
    timeout_after,
    current_task,
)
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


@run_it
async def test_cancel_after_finished():
    """测试一个Bug：当fn在executor中执行完后被cancel，执行结果没有被丢弃，导致syscall收到错误的返回值"""

    async def fn():
        await run_in_thread(lambda: 'OK')  # 进入IO阶段，executor agent执行
        async with timeout_after(0.01) as is_timeout:
            await run_in_thread(lambda: 'EXPIRED')
            # 进入Timer阶段，检查超时
        assert is_timeout
        # 由于Bug，任务栈错位，会导致ret变成EXPIRED
        ret = await run_in_thread(lambda: 'OK')
        assert ret == 'OK'  # 正常情况应该返回OK

    async def main():
        task = await spawn(fn())
        await run_in_thread(lambda: 'OK')  # 进入IO阶段，executor agent执行
        for i in range(10):  # 等待fn执行run_in_thread
            await current_task()
        time.sleep(0.2)  # 模拟大量任务导致kernel loop耗时较长
        await task.join()
        assert not task.error, task.error

    await main()
