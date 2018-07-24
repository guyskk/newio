import asyncio
from functools import partial
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from .syscall import Task, Timer, _set_kernel


def run(coro):
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    kernel = Kernel(coro, loop=loop)
    return kernel.run()


class Kernel:
    def __init__(self, coro, *, loop):
        self.coro = coro
        self.loop = loop
        self.thread_executor = ThreadPoolExecutor(100)
        self.process_executor = ProcessPoolExecutor()

    async def main(self):
        try:
            return await self.coro
        finally:
            self.loop.stop()

    def run(self):
        _set_kernel(self)
        asyncio.set_event_loop(self.loop)
        main_task = self.loop.create_task(self.main())
        try:
            self.loop.run_forever()
            return main_task.result()
        finally:
            asyncio.set_event_loop(None)
            _set_kernel(None)
            self.close()

    def close(self):
        self.loop.close()
        self.thread_executor.shutdown(wait=False)
        self.process_executor.shutdown(wait=False)

    def syscall(self, fut, call, *args):
        handler = getattr(self, call, None)
        if not handler:
            raise RuntimeError(f'unknown syscall {call}')
        handler(fut, *args)

    def nio_sleep(self, current, seconds):
        current._wait(asyncio.sleep(seconds))

    def nio_run_in_thread(self, current, fn, args, kwargs):
        _fn = partial(fn, *args, **kwargs)
        fut = self.loop.run_in_executor(self.thread_executor, _fn)
        current._wait(fut)

    def nio_run_in_process(self, current, fn, args, kwargs):
        _fn = partial(fn, *args, **kwargs)
        fut = self.loop.run_in_executor(self.process_executor, _fn)
        current._wait(fut)

    def nio_wait_read(self, current, fd):
        def callback():
            current.set_result(None)
            self.loop.remove_reader(fd)

        self.loop.add_reader(fd, callback)

    def nio_wait_write(self, current, fd):
        def callback():
            current.set_result(None)
            self.loop.remove_writer(fd)

        self.loop.add_writer(fd, callback)

    def nio_set_timeout(self, current, seconds):
        def callback():
            current.cancal()

        aio_timer = self.loop.call_later(seconds, callback)
        timer = Timer(aio_timer)
        current.set_result(timer)

    def nio_unset_timeout(self, current, timer):
        timer._aio_timer.cancel()
        current.set_result(None)

    def nio_spawn(self, current, coro):
        aio_task = self.loop.create_task(coro)
        task = Task(aio_task)
        aio_task._newio_task = task
        current.set_result(task)

    def nio_current_task(self, current):
        aio_task = asyncio.current_task()
        current.set_result(aio_task._newio_task)

    def nio_cancel(self, current, task):
        task._aio_task.cancal()
        current.set_result(None)

    def nio_join(self, current, task):
        current._wait(task._aio_task)

    def nio_condition_wait(self, current, condition):
        _init_condition(condition)
        current._wait(_cond_wait(condition._aio_contition))

    def nio_condition_notify(self, current, condition, n=1):
        _init_condition(condition)
        current._wait(_cond_notify(condition._aio_contition, n))

    def nio_condition_notify_all(self, current, condition):
        _init_condition(condition)
        current._wait(_cond_notify_all(condition._aio_contition))


def _init_condition(self, condition):
    if condition._aio_contition is None:
        condition._aio_contition = asyncio.Condition()


async def _cond_wait(cond):
    await cond.acquire()
    try:
        await cond.wait()
    finally:
        cond.release()


async def _cond_notify(cond, n):
    await cond.acquire()
    try:
        cond.notify(n)
    finally:
        cond.release()


async def _cond_notify_all(cond):
    await cond.acquire()
    try:
        cond.notify_all()
    finally:
        cond.release()
