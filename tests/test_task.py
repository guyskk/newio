import newio.api as nio

from .helper import run_it


@run_it
async def test_current_task():
    task = await nio.spawn(nio.current_task())
    await task.join()
    assert task.result is task
