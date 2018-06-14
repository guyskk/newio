'''Newio common API for users'''
from . import syscall
from .syscall import TaskCanceled, TaskTimeout, Timer, Task, Lounge

__all__ = (
    'TaskCanceled', 'TaskTimeout', 'Task', 'Timer', 'Lounge',
    'wait_read', 'wait_write', 'sleep', 'spawn', 'current_task',
    'run_in_thread', 'run_in_process', 'run_in_asyncio',
    'timeout_after', 'open_nursery',
)

wait_read = syscall.nio_wait_read
wait_write = syscall.nio_wait_write
run_in_thread = syscall.nio_run_in_thread
run_in_process = syscall.nio_run_in_process
run_in_asyncio = syscall.nio_run_in_asyncio
sleep = syscall.nio_sleep
spawn = syscall.nio_spawn
current_task = syscall.nio_current_task


class timeout_after:
    '''Async context manager for task timeout

    Usage:

        async with timeout_after(3) as is_timeout:
            await sleep(5)
        if is_timeout:
            # task timeout
    '''

    def __init__(self, seconds: float):
        self._seconds = seconds
        self._timer = None

    def __bool__(self):
        return self._timer is not None and self._timer.is_expired

    async def __aenter__(self):
        self._timer = await syscall.nio_timeout_after(self._seconds)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await syscall.nio_unset_timer(self._timer)
        should_suppress = (
            exc_value is not None and
            isinstance(exc_value, TaskTimeout) and
            exc_value.timer is self._timer
        )
        return should_suppress


def open_nursery():
    return _Nursery()


class _Nursery:
    '''Nursery is manager of tasks, it will take care of it spawned tasks.

    All tasks spawned by the nursery are ensure stoped after nursery exited.
    When nursery exiting, it will join spawned tasks, if join failed
    it will cancel spawned tasks.
    '''

    def __init__(self):
        self._tasks = []
        self._is_closed = False

    async def spawn(self, coro):
        '''Spawn task in the nursery'''
        if self._is_closed:
            raise RuntimeError('nursery already closed')
        task = await spawn(coro)
        self._tasks.append(task)
        return task

    async def _join(self):
        for task in self._tasks:
            if task.is_alive:
                await task.join()

    async def _cancel(self):
        for task in self._tasks:
            if task.is_alive:
                await task.cancel()

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('nursery already closed')
        return self

    async def __aexit__(self, *exc_info):
        self._is_closed = True
        try:
            await self._join()
        finally:
            await self._cancel()
            await self._join()
