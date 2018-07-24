import time
import sys

sys.path.insert(0, '..')

from newio_kernel import init_logging
from newio2 import run, nio_run_in_thread, nio_spawn

init_logging('asyncio', level='DEBUG')


async def executor_sleep(seconds):
    begin = time.monotonic()
    await nio_run_in_thread(time.sleep, seconds)
    cost = time.monotonic() - begin
    print(f'sleep cost={cost:.3f} expect={seconds:.3f}')


async def main():
    tasks = []
    for _ in range(20):
        for i in range(20):
            task = await nio_spawn(executor_sleep(i / 3))
            tasks.append(task)
    for task in tasks:
        await task.join()


if __name__ == '__main__':
    run(main())
