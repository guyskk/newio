import os
import asyncio
import inspect
from functools import partial
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import aiomonitor

from ._syscall import Task, _set_kernel
from .contextlib import suppress


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


def _get_name(obj):
    name = getattr(obj, '__qualname__', None)
    if not name:
        name = getattr(obj, '__name__', None)
    if not name:
        name = str(obj)
    if not name:
        name = repr(obj)
    return name


class Kernel:
    def __init__(self, *, loop_policy=None, monitor=None, debug=None):
        if loop_policy is None:
            loop_policy = asyncio.get_event_loop_policy()
        loop = loop_policy.new_event_loop()
        if debug is not None:
            loop.set_debug(debug)
        asyncio.set_event_loop(loop)
        self.loop = loop
        self.monitor = monitor
        self.thread_executor = ThreadPoolExecutor(DEFAULT_MAX_NUM_THREAD)
        self.process_executor = ProcessPoolExecutor(DEFAULT_MAX_NUM_PROCESS)

    def run(self, corofunc, *args, **kwargs):
        main_task = self._create_task(corofunc, *args, **kwargs)
        _set_kernel(self)
        try:
            return self._run_complete(main_task)
        finally:
            _set_kernel(None)
            self.close()

    def _run_complete(self, main_task):
        aio_main_task = main_task._aio_task
        try:
            if self.monitor:
                with aiomonitor.start_monitor(loop=self.loop):
                    self.loop.run_until_complete(aio_main_task)
            else:
                self.loop.run_until_complete(aio_main_task)
        except BaseException:
            # 处理KeyboardInterrupt等异常，保证main_task完整运行结束
            aio_main_task.cancel()
            if not aio_main_task.done():
                try:
                    self.loop.run_until_complete(aio_main_task)
                except asyncio.CancelledError:
                    pass  # ignore
            raise
        else:
            return aio_main_task.result()

    def close(self):
        self.loop.close()
        self.thread_executor.shutdown(wait=False)
        self.process_executor.shutdown(wait=False)

    def _create_task(self, corofunc, *args, **kwargs):
        if inspect.isawaitable(corofunc):
            name = _get_name(corofunc)
            coro = corofunc
        else:
            name = corofunc.__module__ + '.' + _get_name(corofunc)
            coro = corofunc(*args, **kwargs)
        aio_task = self.loop.create_task(coro)
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
            # 大量连续IO的情况下，需要避免频繁注册-取消FD，否则性能会明显下降
            # 解决方案：第一次回调时不取消注册FD，如果后续还有IO，则当前callback会被替换掉；
            # 如果后续没有IO，则callback会再次执行，此时取消注册。
            # 参考：asyncio.selector_events.sock_recv的实现，以及curio kernel关于unregister的注释
            if fut.done():
                self.loop.remove_reader(fd)
            else:
                fut.set_result(None)

        self.loop.add_reader(fd, callback)
        return await fut

    async def nio_wait_write(self, current, fd):
        fut = self.loop.create_future()

        def callback():
            # 原理同nio_wait_read
            if fut.done():
                self.loop.remove_writer(fd)
            else:
                fut.set_result(None)

        self.loop.add_writer(fd, callback)
        return await fut

    async def nio_spawn(self, current, corofunc, args, kwargs):
        return self._create_task(corofunc, *args, **kwargs)

    async def nio_current_task(self, current):
        return current

    async def nio_cancel(self, current, task):
        task._aio_task.cancel()
        with suppress(asyncio.CancelledError):
            await task._aio_task

    async def nio_join(self, current, task):
        return await task._aio_task
