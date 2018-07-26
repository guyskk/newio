from newio import spawn
from newio.queue import Queue
from .helper import run_it


@run_it
async def test_queue():
    queue = Queue(maxsize=2)

    async def producer():
        for i in range(100):
            await queue.put(i)

    async def consumer():
        count = 0
        while True:
            i = await queue.get()
            if i is None:
                break
            count += 1
        return count

    p1 = await spawn(producer())
    p2 = await spawn(producer())
    c1 = await spawn(consumer())
    c2 = await spawn(consumer())
    c3 = await spawn(consumer())
    await p1.join()
    await p2.join()
    for i in range(3):
        await queue.put(None)
    results = [await c1.join(), await c2.join(), await c3.join()]
    assert sum(results) == 200
