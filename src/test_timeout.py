import asyncio
from contextlib import suppress
from async_timeout import timeout

loop = asyncio.get_event_loop()


async def sleep(seconds):
    with suppress(asyncio.TimeoutError):
        async with timeout(seconds) as cm:
            await asyncio.sleep(1000)
    print(cm.expired, seconds)


async def main():
    tasks = []
    for _ in range(2):
        for i in range(2):
            tasks.append(loop.create_task(sleep(i / 3)))
    await asyncio.wait(tasks)


loop.run_until_complete(main())
