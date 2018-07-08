import pytest

from newio import spawn, run_in_thread, run_in_asyncio
from newio.channel import Channel

from .helper import run_it


async def thread_producer(channel):
    def _producer():
        for _ in range(10):
            channel.thread_send(1)

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


async def newio_consumer(channel):
    total = 0
    async for i in channel:
        total += i
    return total


producers = [thread_producer, newio_producer, asyncio_producer]
consumers = [thread_consumer, newio_consumer, asyncio_consumer]


@pytest.mark.parametrize('producer', producers)
@pytest.mark.parametrize('num_producer', [1, 10])
@pytest.mark.parametrize('consumer', consumers)
@pytest.mark.parametrize('num_consumer', [1, 10])
@run_it
async def test_channel(producer, num_producer, consumer, num_consumer):
    producers = []
    consumers = []
    async with Channel() as channel:
        for _ in range(num_producer):
            producers.append(await spawn(producer(channel)))
        for _ in range(num_consumer):
            consumers.append(await spawn(consumer(channel)))
        for task in producers:
            await task.join()
            assert not task.error
        channel.close()
        for task in consumers:
            await task.join()
            assert not task.error
    total = sum([t.result for t in consumers])
    assert total == len(producers) * 10


@pytest.mark.parametrize('num_producer', [1, 10])
@pytest.mark.parametrize('num_consumer', [1, 10])
@pytest.mark.parametrize('bufsize', [1, 10])
@run_it
async def test_mix_channel(num_producer, num_consumer, bufsize):
    producers = []
    consumers = []
    async with Channel(bufsize) as channel:
        for _ in range(num_producer):
            for producer in producers:
                producers.append(await spawn(producer(channel)))
        for _ in range(num_consumer):
            for consumer in consumers:
                consumers.append(await spawn(consumer(channel)))
        for task in producers:
            await task.join()
            assert not task.error
        channel.close()
        for task in consumers:
            await task.join()
            assert not task.error
    total = sum([t.result for t in consumers])
    assert total == len(producers) * 10
