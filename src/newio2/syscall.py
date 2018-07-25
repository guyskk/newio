
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


class TaskCanceled(BaseException):
    """
    Exception raised when task canceled.

    It's used to unwind coroutine stack and cleanup resources,
    the exception **MUST NOT** be catch without reraise.

    The exception directly inherits from BaseException instead of Exception
    so as to not be accidentally caught by code that catches Exception.
    """


class Task:
    def __init__(self, name, aio_task):
        self._name = name
        self._aio_task = aio_task
        self._exception = None

    def __repr__(self):
        return f'<Task {self._name} at {hex(id(self))}>'

    async def join(self):
        return await nio_join(self)

    async def cancel(self):
        return await nio_cancel(self)


class Condition:
    def __init__(self):
        self._aio_condition = None

    async def wait(self):
        return await nio_condition_wait(self)

    async def notify(self, n=1):
        return await nio_condition_notify(self)

    async def notify_all(self):
        return await nio_condition_notify_all(self)


async def nio_wait_read(fd: int) -> None:
    """Wait until fd readable

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await _syscall(nio_wait_read, fd)


async def nio_wait_write(fd: int) -> None:
    """Wait until fd writeable

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await _syscall(nio_wait_write, fd)


async def nio_spawn(coro) -> Task:
    """Spawn a task

    Raises:
        TaskCanceled: task canceled
    """
    return await _syscall(nio_spawn, coro)


async def nio_current_task() -> Task:
    """Get current task

    Raises:
        TaskCanceled: task canceled
    """
    return await _syscall(nio_current_task)


async def nio_cancel(task: Task) -> None:
    """Cancel a task

    This call will return immediately, before task stoped. This call will not
    raise TaskCanceled, so it's safe to cancel multiple tasks one by one.
    """
    return await _syscall(nio_cancel, task)


async def nio_join(task: Task) -> None:
    """Join a task, wait until the task stoped

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await _syscall(nio_join, task)


async def nio_sleep(seconds: float = 0) -> None:
    """Sleep at least <sesonds> seconds

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await _syscall(nio_sleep, seconds)


async def nio_condition_wait(condition: Condition) -> None:
    """Wait on condition

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await _syscall(nio_condition_wait, condition)


async def nio_condition_notify(condition: Condition, n: int = 1) -> None:
    """Nofify at most n tasks waiting on condition

    Raises:
        TaskCanceled: task canceled
    """
    return await _syscall(nio_condition_notify, condition, n)


async def nio_condition_notify_all(condition: Condition) -> None:
    """Nofify at most n tasks waiting on condition

    Raises:
        TaskCanceled: task canceled
    """
    return await _syscall(nio_condition_notify_all, condition)


async def nio_run_in_thread(fn, *args, **kwargs):
    """Run fn in thread executor

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await _syscall(nio_run_in_thread, fn, args, kwargs)


async def nio_run_in_process(fn, *args, **kwargs):
    """Run fn in process executor

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await _syscall(nio_run_in_process, fn, args, kwargs)
