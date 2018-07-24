import time
import asyncio
from functools import partial
from concurrent.futures import ThreadPoolExecutor

from newio_kernel import init_logging

init_logging('asyncio', level='DEBUG')

loop = asyncio.get_event_loop()
loop.set_debug(True)
executor = ThreadPoolExecutor(100)


async def executor_sleep(seconds):
    begin = time.monotonic()
    fut = loop.run_in_executor(executor, partial(time.sleep, seconds))
    await fut
    cost = time.monotonic() - begin
    print(f'sleep cost={cost:.3f} expect={seconds:.3f}')


async def main():
    tasks = []
    for _ in range(10):
        for i in range(10):
            task = loop.create_task(executor_sleep(i / 3))
            tasks.append(task)
    for task in tasks:
        await task
    loop.stop()


if __name__ == '__main__':
    task = loop.create_task(main())
    loop.run_forever()
    loop.close()
