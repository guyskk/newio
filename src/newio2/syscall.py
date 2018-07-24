from asyncio import futures


_kernel = None


def _set_kernel(kernel):
    global _kernel
    if _kernel and kernel is not None:
        raise RuntimeError('kernel already running!')
    _kernel = kernel


def _get_kernel():
    global _kernel
    return _kernel


class NewioFuture(futures.Future):
    def __init__(self, call, *args):
        super().__init__()
        _get_kernel().syscall(self, call.__name__, *args)

    def _wait(self, fut):
        futures._chain_future(fut, self)


class TaskCanceled(BaseException):
    """
    Exception raised when task canceled.

    It's used to unwind coroutine stack and cleanup resources,
    the exception **MUST NOT** be catch without reraise.

    The exception directly inherits from BaseException instead of Exception
    so as to not be accidentally caught by code that catches Exception.
    """


class TaskTimeout(Exception):
    """Exception raised when task timeout."""

    def __init__(self, timer):
        self.timer = timer


class Timer:
    def __init__(self, aio_timer):
        self._aio_timer = aio_timer


class Task:
    def __init__(self, aio_task):
        self._aio_task = aio_task

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
    return await NewioFuture(nio_wait_read, fd)


async def nio_wait_write(fd: int) -> None:
    """Wait until fd writeable

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_wait_write, fd)


async def nio_spawn(coro) -> Task:
    """Spawn a task

    Raises:
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_spawn, coro)


async def nio_current_task() -> Task:
    """Get current task

    Raises:
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_current_task)


async def nio_cancel(task: Task) -> None:
    """Cancel a task

    This call will return immediately, before task stoped. This call will not
    raise TaskCanceled, so it's safe to cancel multiple tasks one by one.
    """
    return await NewioFuture(nio_cancel, task)


async def nio_join(task: Task) -> None:
    """Join a task, wait until the task stoped

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_join, task)


async def nio_sleep(seconds: float = 0) -> None:
    """Sleep at least <sesonds> seconds

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_sleep, seconds)


async def nio_set_timeout(seconds: float) -> Timer:
    """Set a timer for timeout current task after <seconds> seconds

    Raises:
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_set_timeout, seconds)


async def nio_unset_timeout(timer: Timer) -> None:
    """Unset a timer for current task

    Raises:
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_unset_timeout, timer)


async def nio_condition_wait(condition: Condition) -> None:
    """Wait on condition

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_condition_wait, condition)


async def nio_condition_notify(condition: Condition, n: int = 1) -> None:
    """Nofify at most n tasks waiting on condition

    Raises:
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_condition_notify, condition, n)


async def nio_condition_notify_all(condition: Condition) -> None:
    """Nofify at most n tasks waiting on condition

    Raises:
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_condition_notify_all, condition)


async def nio_run_in_thread(fn, *args, **kwargs):
    """Run fn in thread executor

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_run_in_thread, fn, args, kwargs)


async def nio_run_in_process(fn, *args, **kwargs):
    """Run fn in process executor

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    """
    return await NewioFuture(nio_run_in_process, fn, args, kwargs)
