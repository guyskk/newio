import newio as nio
import time

from .helper import run_it


@run_it
async def test_sleep():
    begin = time.monotonic()
    tasks = []
    for i in range(5):
        task = await nio.spawn(nio.sleep(0.2))
        tasks.append(task)
    for task in tasks:
        await task.join()
    cost = time.monotonic() - begin
    assert cost >= 0.2 and cost < 0.3
