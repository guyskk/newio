from newio import syscall
from newio.syscall import FileDescriptor, Timeout, TaskState, Task, Event

__all__ = (
    'FileDescriptor', 'Timeout', 'TaskState', 'Task', 'Event',
)


async def wait_read(fd: FileDescriptor):
    await syscall.nio_wait_read(fd)


async def wait_write(fd: FileDescriptor):
    await syscall.nio_wait_write(fd)


class timeout:
    def __init__(self, seconds):
        self._timeout = Timeout(seconds)

    async def __aenter__(self):
        await syscall.nio_set_timeout(self.timeout)

    async def __aexit__(self, *exc_info):
        await syscall.nio_unset_timeout(self.timeout)


async def sleep(seconds: float):
    await syscall.nio_sleep(seconds)


async def spawn(coro, daemon=False):
    task = Task(coro, daemon=daemon)
    await syscall.nio_spawn(task)
    return task


async def current_task() -> Task:
    return await syscall.nio_current_task()


async def cancel(task: Task):
    await syscall.nio_cancel(task)
