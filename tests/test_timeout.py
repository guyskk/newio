import newio as nio
import time

from .helper import run_it


@run_it
async def test_timeout():
    begin = time.monotonic()
    task = await nio.spawn(nio.sleep(1))
    async with nio.timeout_after(0.1) as is_timeout:
        await task.join()
    if is_timeout:
        print('is_timeout')
        await task.cancel()
    else:
        print('not is_timeout')
    await task.join()
    cost = time.monotonic() - begin
    assert cost >= 0.1 and cost < 0.15
