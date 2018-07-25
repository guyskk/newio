import os
import asyncio
from functools import partial
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import aiomonitor

from .syscall import Task, _set_kernel, TaskCanceled


class Runner:
    """coroutine runner"""

    def __init__(self, *args, **kwargs):
        self.k_args = args
        self.k_kwargs = kwargs

    def __call__(self, *args, **kwargs):
        kernel = Kernel(*self.k_args, **self.k_kwargs)
        return kernel.run(*args, **kwargs)


DEFAULT_MAX_NUM_PROCESS = os.cpu_count() * 2
DEFAULT_MAX_NUM_THREAD = DEFAULT_MAX_NUM_PROCESS * 16


def _get_task_name(coro):
    name = getattr(coro, '__qualname__', None)
    if name is None:
        name = getattr(coro, '__name__', None)
    if name is None:
        return str(coro)
    return name


class Kernel:
    def __init__(self, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        loop.set_debug(True)
        self.loop = loop
        self.thread_executor = ThreadPoolExecutor(DEFAULT_MAX_NUM_THREAD)
        self.process_executor = ProcessPoolExecutor(DEFAULT_MAX_NUM_PROCESS)

    async def main(self, coro):
        try:
            return await coro
        finally:
            self.loop.stop()

    def run(self, coro):
        _set_kernel(self)
        asyncio.set_event_loop(self.loop)
        main_task = self._create_task(self.main(coro))
        try:
            with aiomonitor.start_monitor(loop=self.loop):
                self.loop.run_forever()
            return main_task._aio_task.result()
        finally:
            asyncio.set_event_loop(None)
            _set_kernel(None)
            self.close()

    def close(self):
        self.loop.close()
        self.thread_executor.shutdown(wait=False)
        self.process_executor.shutdown(wait=False)

    def _create_task(self, coro):
        aio_task = self.loop.create_task(coro)
        name = _get_task_name(coro)
        task = Task(name, aio_task)
        aio_task._newio_task = task
        return task

    async def syscall(self, call, *args):
        handler = getattr(self, call, None)
        if not handler:
            raise RuntimeError(f'unknown syscall {call}')
        aio_task = asyncio.Task.current_task()
        current = getattr(aio_task, '_newio_task', None)
        if current is None:
            raise RuntimeError('syscall only available for newio task!')
        print(f'syscall {current} {call} {args}')
        return await handler(current, *args)

    async def nio_sleep(self, current, seconds):
        return await asyncio.sleep(seconds)

    async def nio_run_in_thread(self, current, fn, args, kwargs):
        _fn = partial(fn, *args, **kwargs)
        return await self.loop.run_in_executor(self.thread_executor, _fn)

    async def nio_run_in_process(self, current, fn, args, kwargs):
        _fn = partial(fn, *args, **kwargs)
        return await self.loop.run_in_executor(self.process_executor, _fn)

    async def nio_wait_read(self, current, fd):
        fut = self.loop.create_future()

        def callback():
            fut.set_result(None)
            self.loop.remove_reader(fd)

        self.loop.add_reader(fd, callback)
        return await fut

    async def nio_wait_write(self, current, fd):
        fut = self.loop.create_future()

        def callback():
            fut.set_result(None)
            self.loop.remove_writer(fd)

        self.loop.add_writer(fd, callback)
        return await fut

    async def nio_spawn(self, current, coro):
        return self._create_task(coro)

    async def nio_current_task(self, current):
        return current

    async def nio_cancel(self, current, task):
        task._exception = TaskCanceled()
        task._aio_task.cancal()

    async def nio_join(self, current, task):
        try:
            return await task._aio_task
        except asyncio.CancelledError:
            if task._exception is not None:
                raise task._exception from None
            raise

    async def nio_condition_wait(self, current, condition):
        _init_condition(condition)
        cond = condition._aio_contition
        await cond.acquire()
        try:
            await cond.wait()
        finally:
            cond.release()

    async def nio_condition_notify(self, current, condition, n=1):
        _init_condition(condition)
        cond = condition._aio_contition
        await cond.acquire()
        try:
            cond.notify(n)
        finally:
            cond.release()

    async def nio_condition_notify_all(self, current, condition):
        _init_condition(condition)
        cond = condition._aio_contition
        await cond.acquire()
        try:
            cond.notify_all()
        finally:
            cond.release()


def _init_condition(self, condition):
    if condition._aio_contition is None:
        condition._aio_contition = asyncio.Condition()
