"""Newio common API for users"""
import asyncio
import async_timeout
from asyncio import CancelledError

from . import _syscall
from ._syscall import Task
from ._kernel import Runner

run = Runner()


__all__ = (
    'Task',
    'Runner',
    'CancelledError',
    'run',
    'wait_read',
    'wait_write',
    'sleep',
    'spawn',
    'current_task',
    'run_in_thread',
    'run_in_process',
    'timeout_after',
    'open_nursery',
)

wait_read = _syscall.nio_wait_read
wait_write = _syscall.nio_wait_write
run_in_thread = _syscall.nio_run_in_thread
run_in_process = _syscall.nio_run_in_process
sleep = _syscall.nio_sleep
spawn = _syscall.nio_spawn
current_task = _syscall.nio_current_task


def timeout_after(seconds: float):
    """Async context manager for task timeout

    Usage:

        async with timeout_after(3) as is_timeout:
            await sleep(5)
        if is_timeout:
            # task timeout
    """
    return Timeout(seconds)


class Timeout:
    def __init__(self, seconds: float):
        self._seconds = seconds
        self._timer = async_timeout.timeout(seconds)

    def __bool__(self):
        return self._timer.expired

    def __repr__(self):
        return f'<Timeout {bool(self)} at {hex(id(self))}>'

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
    """Nursery is manager of tasks, it will take care of it spawned tasks.

    All tasks spawned by the nursery are ensure stoped after nursery exited.
    When nursery exiting, it will join spawned tasks, if nursery raises,
    it will cancel spawned tasks.
    """
    return Nursery()


class Nursery:
    def __init__(self):
        self._tasks = []
        self._is_closed = False

    async def spawn(self, corofunc, *args, **kwargs):
        """Spawn task in the nursery"""
        if self._is_closed:
            raise RuntimeError('nursery already closed')
        task = await spawn(corofunc, *args, **kwargs)
        self._tasks.append(task)
        return task

    async def _join(self):
        for task in self._tasks:
            await task.join()

    async def _cancel(self):
        for task in self._tasks:
            await task.cancel()

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('nursery already closed')
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self._is_closed = True
        if exc_value is None:
            try:
                await self._join()
            except Exception:
                await self._cancel()
                raise
        else:
            await self._cancel()
