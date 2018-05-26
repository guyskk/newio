import time
from types import coroutine


class TaskCanceled(Exception):
    '''Task canceled'''


class TaskTimeout(Exception):
    '''Task timeout'''


class FileDescriptor:
    '''
    Wrapper class around an integer file descriptor. This is used
    to take advantage of an I/O scheduling performance optimization
    in the kernel. If a non-integer file object is given, the
    kernel is able to reuse prior registrations on the event loop.
    The reason this wrapper class is used is that even though an
    integer file descriptor might be reused by the host OS,
    instances of FileDescriptor will not be reused.
    Thus, if a file is closed and a new file opened on the same descriptor,
    it will be detected as a different file.

    See also: https://github.com/dabeaz/curio/issues/104
    '''
    __slots__ = ('_fileno',)

    def __init__(self, fileno: int):
        self._fileno = fileno
        self._nio_ref_ = None

    def fileno(self) -> int:
        return self._fileno

    def __repr__(self):
        return '<fd={!r}>'.format(self.fileno)


class Timeout:

    def __init__(self, kernel_timeout):
        self._nio_ref_ = kernel_timeout
        kernel_timeout._nio_ref_ = self

    @property
    def seconds(self) -> float:
        return self._nio_ref_.seconds

    @property
    def deadline(self) -> float:
        return self._nio_ref_.deadline

    @property
    def is_expired(self) -> bool:
        return self._nio_ref_.is_expired

    def __repr__(self):
        if self.is_expired:
            status = 'expired'
        else:
            remain = max(0, self.deadline - time.monotonic())
            status = 'remain={:.3f}s'.format(remain)
        return '<Timeout {:.3f}s {}>'.format(self.seconds, status)


class Event:

    def __init__(self):
        self._nio_ref_ = None

    @property
    def is_set(self) -> bool:
        return self._kernel_event.is_set

    async def wait(self):
        await nio_schedule_wait(self)

    async def notify(self):
        await nio_schedule_notify(self)

    def __repr__(self):
        return '<Event@{} is_set={}>'.format(hex(id(self)), self.is_set)


class Task:
    def __init__(self, kernel_task):
        self._nio_ref_ = kernel_task
        kernel_task._nio_ref_ = self

    @property
    def ident(self) -> int:
        return self._nio_ref_.ident

    @property
    def name(self) -> str:
        return self._nio_ref_.name

    @property
    def is_alive(self):
        return self._nio_ref_.is_alive

    @property
    def result(self):
        if self.is_alive:
            raise RuntimeError('task is alive, can not get result')
        return self._nio_ref_.result

    @property
    def error(self):
        if self.is_alive:
            raise RuntimeError('task is alive, can not get error')
        return self._nio_ref_.error

    def __repr__(self):
        if self.is_alive:
            state = 'alive'
        elif self.error:
            state = 'error'
        else:
            state = 'result'
        return '<Task#{} {} @{}>'.format(self.ident, self.name, state)

    async def join(self):
        await nio_join(self)

    async def cancel(self):
        await nio_cancel(self)


@coroutine
def nio_wait_read(fd: FileDescriptor) -> None:
    yield (nio_wait_read, fd)


@coroutine
def nio_wait_write(fd: FileDescriptor) -> None:
    yield (nio_wait_write, fd)


@coroutine
def nio_sleep(seconds: float=0) -> None:
    yield (nio_sleep, seconds)


@coroutine
def nio_spawn(coro) -> Task:
    return (yield (nio_spawn, coro))


@coroutine
def nio_current_task() -> Task:
    return (yield (nio_current_task,))


@coroutine
def nio_cancel(task: Task) -> None:
    yield (nio_cancel, task)


@coroutine
def nio_join(task: Task) -> None:
    yield (nio_join, task)


@coroutine
def nio_set_timeout(seconds) -> Timeout:
    return (yield (nio_set_timeout, seconds))


@coroutine
def nio_unset_timeout(timeout: Timeout) -> None:
    yield (nio_unset_timeout, timeout)


@coroutine
def nio_schedule_wait(event: Event) -> None:
    yield (nio_schedule_wait, event)


@coroutine
def nio_schedule_notify(event: Event) -> None:
    yield (nio_schedule_notify, event)
