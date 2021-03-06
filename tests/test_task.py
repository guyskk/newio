import newio as nio

from .helper import run_it


@run_it
async def test_current_task():
    task = await nio.spawn(nio.current_task())
    result = await task.join()
    assert result is task
