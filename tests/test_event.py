import newio.api as nio

from .helper import run_it


@run_it
async def test_event():
    evt = nio.Event()
    task1 = await nio.spawn(evt.wait())
    task2 = await nio.spawn(task1.join())
    await evt.notify()
    await task2.join()
    assert not task1.is_alive
    assert task1.error is None
    assert task2.error is None
