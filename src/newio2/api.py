"""Newio common API for users"""
from contextlib import suppress
import asyncio
import async_timeout

from . import syscall
from .syscall import TaskCanceled, Task, Condition
from .kernel import Runner

run = Runner()


__all__ = (
    'TaskCanceled',
    'Task',
    'Condition',
    'Runner',
    'run',
    'wait_read',
    'wait_write',
    'sleep',
    'spawn',
    'current_task',
    'run_in_thread',
    'run_in_process',
    'timeout',
    'open_nursery',
)

wait_read = syscall.nio_wait_read
wait_write = syscall.nio_wait_write
run_in_thread = syscall.nio_run_in_thread
run_in_process = syscall.nio_run_in_process
sleep = syscall.nio_sleep
spawn = syscall.nio_spawn
current_task = syscall.nio_current_task


class timeout:
    """Async context manager for task timeout

    Usage:

        async with timeout(3) as is_timeout:
            await sleep(5)
        if is_timeout:
            # task timeout
    """

    def __init__(self, seconds: float):
        self._seconds = seconds
        self._timer = async_timeout.timeout(seconds)

    def __bool__(self):
        return self._timer.expired

    async def __aenter__(self):
        await self._timer.__aenter__()
        return self

    async def __aexit__(self, *exc_info):
        try:
            await self._timer.__aexit__(*exc_info)
        except asyncio.TimeoutError:
            if not self._timer.expired:
                raise
            return True


def open_nursery():
    return _Nursery()


class _Nursery:
    """Nursery is manager of tasks, it will take care of it spawned tasks.

    All tasks spawned by the nursery are ensure stoped after nursery exited.
    When nursery exiting, it will join spawned tasks, if nursery raises,
    it will cancel spawned tasks.
    """

    def __init__(self):
        self._tasks = []
        self._is_closed = False

    async def spawn(self, coro):
        """Spawn task in the nursery"""
        if self._is_closed:
            raise RuntimeError('nursery already closed')
        task = await spawn(coro)
        self._tasks.append(task)
        return task

    async def _join(self):
        for task in self._tasks:
            with suppress(Exception):
                await task.join()

    async def _cancel(self):
        for task in self._tasks:
            with suppress(Exception):
                await task.cancel()

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('nursery already closed')
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self._is_closed = True
        if exc_value is None:
            await self._join()
        else:
            await self._cancel()
