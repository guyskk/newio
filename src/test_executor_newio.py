import time
import sys
import asyncio

sys.path.insert(0, '..')

from newio_kernel import init_logging
from newio2 import run, run_in_thread, spawn, timeout, sleep

init_logging('asyncio', level='DEBUG')


async def executor_sleep(seconds):
    begin = time.monotonic()
    await run_in_thread(time.sleep, seconds)
    cost = time.monotonic() - begin
    print(f'sleep cost={cost:.3f} expect={seconds:.3f}')


async def timeout_sleep(seconds):
    begin = time.monotonic()
    task = asyncio.Task.current_task()
    print('timeout_sleep', task, seconds)
    async with timeout(seconds):
        await sleep(100)
    cost = time.monotonic() - begin
    print(f'timeout_sleep cost={cost:.3f} expect={seconds:.3f}')


async def main():
    tasks = []
    for _ in range(2):
        for i in range(2):
            task = await spawn(executor_sleep(i / 3))
            tasks.append(task)
            task = await spawn(timeout_sleep(i / 3))
            tasks.append(task)
    for task in tasks:
        await task.join()


if __name__ == '__main__':
    run(main())
