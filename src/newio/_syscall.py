"""All syscalls are defined here for communicating with kernel"""

_kernel = None


def _set_kernel(kernel):
    global _kernel
    if _kernel and kernel is not None:
        raise RuntimeError('kernel already running!')
    _kernel = kernel


def _get_kernel():
    global _kernel
    if _kernel is None:
        raise RuntimeError('kernel not running!')
    return _kernel


def _syscall(call, *args):
    return _get_kernel().syscall(call.__name__, *args)


class Task:
    def __init__(self, name, aio_task):
        self._name = name
        self._aio_task = aio_task

    def __repr__(self):
        return f'<Task {self._name} at {hex(id(self))}>'

    async def join(self):
        return await nio_join(self)

    async def cancel(self):
        return await nio_cancel(self)


async def nio_wait_read(fd: int) -> None:
    """Wait until fd readable"""
    return await _syscall(nio_wait_read, fd)


async def nio_wait_write(fd: int) -> None:
    """Wait until fd writeable"""
    return await _syscall(nio_wait_write, fd)


async def nio_spawn(corofunc, *args, **kwargs) -> Task:
    """Spawn a task"""
    return await _syscall(nio_spawn, corofunc, args, kwargs)


async def nio_current_task() -> Task:
    """Get current task"""
    return await _syscall(nio_current_task)


async def nio_cancel(task: Task) -> None:
    """Cancel a task, wait until the task actually terminates"""
    return await _syscall(nio_cancel, task)


async def nio_join(task: Task) -> None:
    """Join a task, wait until the task stopped"""
    return await _syscall(nio_join, task)


async def nio_sleep(seconds: float = 0) -> None:
    """Sleep at least <sesonds> seconds"""
    return await _syscall(nio_sleep, seconds)


async def nio_run_in_thread(fn, *args, **kwargs):
    """Run fn in thread executor"""
    return await _syscall(nio_run_in_thread, fn, args, kwargs)


async def nio_run_in_process(fn, *args, **kwargs):
    """Run fn in process executor"""
    return await _syscall(nio_run_in_process, fn, args, kwargs)
