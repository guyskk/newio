from newio_kernel.kernel import Kernel
import newio.api as nio
import traceback


async def test_sleep():
    print('main before spawn')
    tasks = []
    for i in range(1000):
        task = await nio.spawn(child(0.15 * i))
        tasks.append(task)
    print('main after spawn')
    for task in tasks:
        await task.join()
    print('main exit')


async def child(seconds):
    print('child before sleep')
    await nio.sleep(seconds)
    print('child after sleep')
    print('child exit')


async def dead_coro():
    while True:
        try:
            await nio.sleep(1)
        except:
            print('you can not kill me:P')
            traceback.print_exc()


async def test_cancel():
    print('before nursery')
    async with nio.open_nursery() as nursery:
        print('before spawn')
        await nursery.spawn(dead_coro())
        print('after spawn')
    print('after nursery')

kernel = Kernel(test_cancel())
kernel.run()
