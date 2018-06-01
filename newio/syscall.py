'''All syscalls are defined here for communicating with kernel'''
from types import coroutine


class TaskCanceled(BaseException):
    '''
    Exception raised when task cancelled.

    It's used to unwind coroutine stack and cleanup resources,
    the exception **MUST NOT** be catch without reraise.

    The exception directly inherits from BaseException instead of Exception
    so as to not be accidentally caught by code that catches Exception.
    '''


class TaskTimeout(Exception):
    '''Exception raised when task timeout.'''


@coroutine
def nio_wait_read(fileno: int) -> None:
    '''Wait until fileno readable.

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_wait_read, fileno)


@coroutine
def nio_wait_write(fileno: int) -> None:
    '''Wait until fileno writeable.

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_wait_write, fileno)


@coroutine
def nio_sleep(seconds: float=0) -> None:
    '''Sleep at least <sesonds> seconds.

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_sleep, seconds)


@coroutine
def nio_spawn(coro, timeout: int=None) -> int:
    '''Spawn a task, return task id.

    If timeout is not None, cancel the task after <timeout> seconds.

    Raises:
        TaskCanceled: task canceled
    '''
    return (yield (nio_spawn, coro, timeout))


@coroutine
def nio_current_id() -> int:
    '''Get current task id.

    Raises:
        TaskCanceled: task canceled
    '''
    return (yield (nio_current_id,))


@coroutine
def nio_cancel(task_id: int) -> None:
    '''Cancel a task.

    This call will return before task stoped. This call will not
    raise TaskCanceled, so it's safe to cancel multiple tasks.
    '''
    yield (nio_cancel, task_id)


@coroutine
def nio_join(task_id: int) -> None:
    '''Join a task, wait until the task stoped

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_join, task_id)


@coroutine
def nio_set_timeout(seconds) -> int:
    '''Set a timeout for current task, return a unique token

    Raises:
        TaskCanceled: task canceled
    '''
    return (yield (nio_set_timeout, seconds))


@coroutine
def nio_unset_timeout(token: int) -> None:
    '''Unset a timeout for current task

    Raises:
        TaskCanceled: task canceled
    '''
    yield (nio_unset_timeout, token)


@coroutine
def nio_futex_wait(futex: object) -> None:
    '''Wait on futex - fast userspace mutex, borrowing from Linux kernel.

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_futex_wait, futex)


@coroutine
def nio_futex_wake(futex: object, n: int) -> None:
    '''Wake up at most n tasks waiting on futex.

    Raises:
        TaskCanceled: task canceled
    '''
    yield (nio_futex_wake, futex, n)


@coroutine
def nio_futex_wake_all(futex: object) -> None:
    '''Wake up all tasks waiting on futex.

    Raises:
        TaskCanceled: task canceled
    '''
    yield (nio_futex_wake_all, futex)


@coroutine
def nio_run_in_thread(fn, *args, **kwargs):
    '''Run fn in thread pool.

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    return (yield (nio_run_in_thread, fn, args, kwargs))


@coroutine
def nio_run_in_process(fn, *args, **kwargs):
    '''Run fn in process pool.

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    return (yield (nio_run_in_process, fn, args, kwargs))
