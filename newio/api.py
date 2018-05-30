'''Newio common API for users'''
from . import syscall
from .syscall import TaskCanceled, TaskTimeout, FileDescriptor, Task

__all__ = (
    'TaskCanceled', 'TaskTimeout', 'FileDescriptor', 'Task',
    'wait_read', 'wait_write', 'sleep', 'spawn', 'current_task',
    'timeout', 'open_nursery', 'run_in_thread', 'run_in_process',
)


async def wait_read(fd: FileDescriptor) -> None:
    '''Wait until fd readable'''
    await syscall.nio_wait_read(fd)


async def wait_write(fd: FileDescriptor) -> None:
    '''Wait until fd writeable'''
    await syscall.nio_wait_write(fd)


async def run_in_thread(fn, *args, **kwargs):
    '''Run fn in thread pool'''
    return await syscall.nio_run_in_thread(fn, *args, **kwargs)


async def run_in_process(fn, *args, **kwargs):
    '''Run fn in process pool'''
    return await syscall.nio_run_in_process(fn, *args, **kwargs)


async def sleep(seconds: float) -> None:
    '''Sleep at least <seconds> seconds'''
    await syscall.nio_sleep(seconds)


async def spawn(coro) -> Task:
    '''Spawn a task'''
    return await syscall.nio_spawn(coro)


async def current_task() -> Task:
    '''Get current task'''
    return await syscall.nio_current_task()


class timeout:
    '''Async context manager for task timeout'''

    def __init__(self, seconds: float):
        self._seconds = seconds
        self._timeout = None

    async def __aenter__(self):
        self._timeout = await syscall.nio_set_timeout(self._seconds)

    async def __aexit__(self, *exc_info):
        await syscall.nio_unset_timeout(self._timeout)


class open_nursery:
    '''Nursery is manager of tasks, it will take care of it spawned tasks.

    All tasks spawned by the nursery are ensure stoped after nursery exited.
    You must explicitly join spawned tasks, otherwise they will be
    canceled after nursery exited.
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

    async def join(self):
        for task in self._tasks:
            if task.is_alive:
                await task.join()

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('nursery already closed')
        return self

    async def __aexit__(self, *exc_info):
        self._is_closed = True
        for task in self._tasks:
            if task.is_alive:
                await task.cancel()
        await self.join()
