'''All syscalls are defined here for communicating with kernel

Implementation Note:

    the **_nio_ref_** attributes is used to bind user object and kernel object,
    so as to efficiently exchange between user space and kernel space.
'''
import time
from types import coroutine


class TaskCanceled(BaseException):
    '''
    Exception raised when task canceled.

    It's used to unwind coroutine stack and cleanup resources,
    the exception **MUST NOT** be catch without reraise.

    The exception directly inherits from BaseException instead of Exception
    so as to not be accidentally caught by code that catches Exception.
    '''


class TaskTimeout(Exception):
    '''Exception raised when task timeout.'''

    def __init__(self, timer):
        self.timer = timer


class Timer:
    '''A wrapper for kernel timer object'''

    def __init__(self, kernel_timer):
        self._nio_ref_ = kernel_timer
        kernel_timer._nio_ref_ = self

    @property
    def seconds(self) -> float:
        '''timer expire duration in seconds'''
        return self._nio_ref_.seconds

    @property
    def deadline(self) -> float:
        '''timer deadline, based on time.monotonic()'''
        return self._nio_ref_.deadline

    @property
    def is_expired(self) -> bool:
        '''is timer expired'''
        return self._nio_ref_.is_expired

    @property
    def is_canceled(self) -> bool:
        '''is timer canceled'''
        return self._nio_ref_.is_canceled

    async def cancel(self):
        await nio_unset_timer(self)

    def __repr__(self):
        if self.is_expired:
            status = 'expired'
        elif self.is_canceled:
            status = 'canceled'
        else:
            remain = max(0, self.deadline - time.monotonic())
            status = f'remain={remain:.3f}s'
        return f'<Timer {self.seconds:.3f}s {status}>'


class Lounge:
    '''
    Lounge - waiting room for tasks, borrowing from Linux kernel(Futex).
    It is a sychronization primitive used to coordinate tasks.
    '''

    WAKE_ALL = -1  # A symbol for wake all tasks

    def __init__(self):
        self._nio_ref_ = None

    def __repr__(self):
        return f'<Lounge@{hex(self.ident)[2:]}>'

    @property
    def ident(self) -> int:
        return id(self)

    async def wait(self):
        '''Wait on the lounge'''
        await nio_lounge_wait(self)

    async def wake(self, n: int):
        '''Wake up at most n tasks waiting on the lounge'''
        await nio_lounge_wake(self, n)


class Task:
    '''A wrapper for kernel task'''

    def __init__(self, kernel_task):
        self._nio_ref_ = kernel_task
        kernel_task._nio_ref_ = self

    @property
    def ident(self) -> int:
        '''task id'''
        return self._nio_ref_.ident

    @property
    def name(self) -> str:
        '''task name'''
        return self._nio_ref_.name

    @property
    def is_alive(self) -> bool:
        '''is task alive'''
        return self._nio_ref_.is_alive

    @property
    def result(self):
        '''Get task result after task stoped'''
        if self.is_alive:
            raise RuntimeError('task is alive, can not get result')
        return self._nio_ref_.result

    @property
    def error(self):
        '''Get task error after task stoped'''
        if self.is_alive:
            raise RuntimeError('task is alive, can not get error')
        return self._nio_ref_.error

    async def join(self) -> None:
        '''Join the task'''
        await nio_join(self)

    async def cancel(self) -> None:
        '''Cancel the task'''
        await nio_cancel(self)

    def __repr__(self):
        if self.is_alive:
            state = 'alive'
        elif self.error:
            state = 'error'
        else:
            state = 'stoped'
        return f'<Task#{self.ident} {self.name} @{state}>'


@coroutine
def nio_wait_read(fd: int) -> None:
    '''Wait until fd readable

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_wait_read, fd)


@coroutine
def nio_wait_write(fd: int) -> None:
    '''Wait until fd writeable

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_wait_write, fd)


@coroutine
def nio_spawn(coro, cancel_after: float=None) -> Task:
    '''Spawn a task

    If cancel_after is not None, cancel the task after <cancel_after> seconds

    Raises:
        TaskCanceled: task canceled
    '''
    return (yield (nio_spawn, coro, cancel_after))


@coroutine
def nio_current_task() -> Task:
    '''Get current task

    Raises:
        TaskCanceled: task canceled
    '''
    return (yield (nio_current_task,))


@coroutine
def nio_cancel(task: Task) -> None:
    '''Cancel a task

    This call will return immediately, before task stoped. This call will not
    raise TaskCanceled, so it's safe to cancel multiple tasks one by one.
    '''
    yield (nio_cancel, task)


@coroutine
def nio_join(task: Task) -> None:
    '''Join a task, wait until the task stoped

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_join, task)


@coroutine
def nio_sleep(seconds: float=0) -> None:
    '''Sleep at least <sesonds> seconds

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    if seconds < 0:
        raise ValueError('can not sleep a negative seconds')
    yield (nio_sleep, seconds)


@coroutine
def nio_timeout_after(seconds: float) -> Timer:
    '''Set a timer for timeout current task after <seconds> seconds

    Raises:
        TaskCanceled: task canceled
    '''
    if seconds < 0:
        raise ValueError('can not set negative seconds timer')
    return (yield (nio_timeout_after, seconds))


@coroutine
def nio_unset_timer(timer: Timer) -> None:
    '''Unset a timer for current task

    Raises:
        TaskCanceled: task canceled
    '''
    yield (nio_unset_timer, timer)


@coroutine
def nio_lounge_wait(lounge: Lounge) -> None:
    '''Wait on lounge

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_lounge_wait, lounge)


@coroutine
def nio_lounge_wake(lounge: Lounge, n: int) -> None:
    '''Wake up at most n tasks waiting on lounge

    Raises:
        TaskCanceled: task canceled
    '''
    yield (nio_lounge_wake, lounge, n)


@coroutine
def nio_run_in_thread(fn, *args, **kwargs):
    '''Run fn in thread executor

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    return (yield (nio_run_in_thread, fn, args, kwargs))


@coroutine
def nio_run_in_process(fn, *args, **kwargs):
    '''Run fn in process executor

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    return (yield (nio_run_in_process, fn, args, kwargs))


@coroutine
def nio_run_in_asyncio(coro):
    '''Run fn in asyncio executor

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    return (yield (nio_run_in_asyncio, coro))
