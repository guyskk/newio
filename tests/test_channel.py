import pytest

from newio import spawn, run_in_thread, run_in_asyncio
from newio.channel import Channel

from .helper import run_it


async def thread_producer(channel):
    def _producer():
        for _ in range(10):
            channel.thread_send(1)
        channel.close()

    return await run_in_thread(_producer)


async def thread_consumer(channel):
    def _consumer():
        total = 0
        for i in channel.thread_iter():
            total += i
        return total

    return await run_in_thread(_consumer)


async def asyncio_producer(channel):
    async def _producer():
        for _ in range(10):
            await channel.asyncio_send(1)
        channel.close()

    return await run_in_asyncio(_producer())


async def asyncio_consumer(channel):
    async def _consumer():
        total = 0
        async for i in channel.asyncio_iter():
            total += i
        return total

    return await run_in_asyncio(_consumer())


async def newio_producer(channel):
    for _ in range(10):
        await channel.send(1)
    channel.close()


async def newio_consumer(channel):
    total = 0
    async for i in channel:
        total += i
    return total


producers = [thread_producer, newio_producer, asyncio_producer]
consumers = [thread_consumer, newio_consumer, asyncio_consumer]


@pytest.mark.parametrize('producer', producers)
@pytest.mark.parametrize('consumer', consumers)
@run_it
async def test_channel(producer, consumer):
    async with Channel() as channel:
        producer_task = await spawn(producer(channel))
        consumer_task = await spawn(consumer(channel))
        await producer_task.join()
        await consumer_task.join()
    assert not producer_task.error
    assert not consumer_task.error
    assert consumer_task.result == 10
