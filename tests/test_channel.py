from newio import spawn, run_in_thread
from newio.channel import Channel

from .helper import run_it


@run_it
async def test_thread_to_coroutine():

    def producer(channel):
        for _ in range(100):
            channel.send(1)

    async def consumer(channel):
        total = 0
        async for i in channel:
            total += i
        return total

    async with Channel(maxsize=1) as channel:
        producer_task = await spawn(run_in_thread(producer, channel))
        consumer_task = await spawn(consumer(channel))
        await producer_task.join()

    await consumer_task.join()
    assert consumer_task.result == 100


@run_it
async def test_coroutine_to_thread():

    async def producer(channel):
        for _ in range(100):
            await channel.asend(1)

    def consumer(channel):
        total = 0
        for i in channel:
            total += 1
        return total

    async with Channel(maxsize=1) as channel:
        producer_task = await spawn(producer(channel))
        consumer_task = await spawn(run_in_thread(consumer, channel))
        await producer_task.join()

    await consumer_task.join()
    assert consumer_task.result == 100
