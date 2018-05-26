from newio import syscall
from newio.syscall import (
    TaskCanceled, TaskTimeout,
    FileDescriptor, Timeout, Task, Event)

__all__ = (
    'TaskCanceled', 'TaskTimeout',
    'FileDescriptor', 'Timeout', 'Task', 'Event',
)


async def wait_read(fd: FileDescriptor):
    await syscall.nio_wait_read(fd)


async def wait_write(fd: FileDescriptor):
    await syscall.nio_wait_write(fd)


class timeout:
    def __init__(self, seconds):
        self._seconds = seconds
        self._timeout = None

    async def __aenter__(self):
        self._timeout = await syscall.nio_set_timeout(self._seconds)

    async def __aexit__(self, *exc_info):
        await syscall.nio_unset_timeout(self._timeout)


class open_nursery:

    def __init__(self):
        self.tasks = []

    async def spawn(self, coro):
        task = await spawn(coro)
        self.tasks.append(task)
        return task

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        for task in self.tasks:
            if task.is_alive:
                try:
                    await task.cancel()
                except:
                    pass


async def sleep(seconds: float):
    await syscall.nio_sleep(seconds)


async def spawn(coro):
    return await syscall.nio_spawn(coro)


async def current_task() -> Task:
    return await syscall.nio_current_task()


async def cancel(task: Task):
    await syscall.nio_cancel(task)


async def join(task: Task):
    await syscall.nio_join(task)
