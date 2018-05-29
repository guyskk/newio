'''All syscalls are defined here for communicating with kernel

Implementation Note:

    the **_nio_ref_** attributes is used to bind user object and kernel object,
    so as to efficiently exchange between user space and kernel space.
'''
import time
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


class FileDescriptor:
    '''A wrapper for file descriptor'''

    def __init__(self, fileno: int):
        self._fileno = fileno
        self._nio_ref_ = None

    def fileno(self) -> int:
        '''Get the underlying file descriptor'''
        return self._fileno

    def __repr__(self):
        return f'<FileDescriptor#{self.fileno()}>'


class Timeout:
    '''A wrapper for kernel timeout object'''

    def __init__(self, kernel_timeout):
        self._nio_ref_ = kernel_timeout
        kernel_timeout._nio_ref_ = self

    @property
    def seconds(self) -> float:
        '''timeout seconds'''
        return self._nio_ref_.seconds

    @property
    def deadline(self) -> float:
        '''timeout deadline, based on time.monotonic()'''
        return self._nio_ref_.deadline

    @property
    def is_expired(self) -> bool:
        '''is timeout expired'''
        return self._nio_ref_.is_expired

    def __repr__(self):
        if self.is_expired:
            status = 'expired'
        else:
            remain = max(0, self.deadline - time.monotonic())
            status = f'remain={remain:.3f}s'
        return f'<Timeout {self.seconds:.3f}s {status}>'


class Futex:
    '''
    Futex - fast userspace mutex, borrowing from Linux kernel.
    It is a sychronization primitive used to coordinate tasks.
    '''

    # A symbol for wake all tasks
    WAKE_ALL = -1

    def __init__(self):
        self._nio_ref_ = None

    def __repr__(self):
        return f'<Futex@{hex(id(self))}>'

    async def wait(self):
        '''Wait on the futex'''
        await nio_futex_wait(self)

    async def wake(self, n: int):
        '''Wake up at most n tasks waiting on the futex'''
        await nio_futex_wake(self, n)


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
def nio_wait_read(fd: FileDescriptor) -> None:
    '''Wait until fd readable

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_wait_read, fd)


@coroutine
def nio_wait_write(fd: FileDescriptor) -> None:
    '''Wait until fd writeable

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_wait_write, fd)


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
def nio_spawn(coro) -> Task:
    '''Spawn a task

    Raises:
        TaskCanceled: task canceled
    '''
    return (yield (nio_spawn, coro))


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
def nio_set_timeout(seconds) -> Timeout:
    '''Set a timeout for current task

    Raises:
        TaskCanceled: task canceled
    '''
    if seconds < 0:
        raise ValueError('can not set negative timeout')
    return (yield (nio_set_timeout, seconds))


@coroutine
def nio_unset_timeout(timeout: Timeout) -> None:
    '''Unset a timeout for current task

    Raises:
        TaskCanceled: task canceled
    '''
    yield (nio_unset_timeout, timeout)


@coroutine
def nio_futex_wait(futex: Futex) -> None:
    '''Wait on futex

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield (nio_futex_wait, futex)


@coroutine
def nio_futex_wake(futex: Futex, n: int) -> None:
    '''Wake up at most n tasks waiting on futex

    Raises:
        TaskCanceled: task canceled
    '''
    yield (nio_futex_wake, futex, n)


@coroutine
def nio_run_in_thread(fn, *args, **kwargs):
    '''Run fn in thread pool

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield(nio_run_in_thread, fn, args, kwargs)


@coroutine
def nio_run_in_process(fn, *args, **kwargs):
    '''Run fn in process pool

    Raises:
        TaskTimeout: task timeout
        TaskCanceled: task canceled
    '''
    yield(nio_run_in_process, fn, args, kwargs)
