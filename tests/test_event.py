import newio as nio
from newio.sync import Event
from .helper import run_it


@run_it
async def test_event():
    evt = Event()
    task = await nio.spawn(evt.wait())
    await evt.set()
    await task.join()
