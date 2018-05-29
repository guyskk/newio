import newio as nio
import time

from .helper import run_it


@run_it
async def test_timeout():
    begin = time.monotonic()
    task = await nio.spawn(nio.sleep(1))
    async with nio.timeout(0.1):
        try:
            await task.join()
        except nio.TaskTimeout as ex:
            await task.cancel()
    await task.join()
    cost = time.monotonic() - begin
    assert cost >= 0.1 and cost < 0.15
