from types import coroutine
from enum import Enum


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

    @property
    def fileno(self) -> int:
        return self._fileno

    def __repr__(self):
        return '<fd={!r}>'.format(self.fileno)


class Timeout:
    __slots__ = ('_seconds',)

    def __init__(self, seconds: float):
        self._seconds = seconds
        self._set = False

    @property
    def seconds(self) -> float:
        return self._seconds

    @property
    def is_set(self):
        return self._set

    async def _nio_on_set_(self):
        self._set = True

    async def _nio_on_unset_(self):
        self._set = False

    def __repr__(self):
        return '<Timeout {:.3f}s>'.format(self.seconds)


class Event:

    def __init__(self):
        self._set = False

    async def _nio_on_set_(self):
        self._set = True

    async def _nio_on_unset_(self):
        self._set = False

    @property
    def is_set(self) -> bool:
        '''check is event set'''
        return self._set

    async def wait(self):
        await nio_schedule_wait(self)

    async def notify(self):
        await nio_schedule_notify(self)


class TaskState(Enum):
    INIT = 'init'
    RUNNING = 'running'
    STOPED = 'stoped'


class Task:
    '''
    The Task class wraps a coroutine and provides some additional attributes
    related to execution state and debugging.  Tasks are not normally
    instantiated directly. Instead, use spawn().
    '''

    def __init__(self, coro, daemon=False):
        self._coro = coro
        self._daemon = daemon
        self._name = getattr(coro, '__qualname__', str(coro))
        self._ident = None
        self._state = TaskState.INIT
        self._result = None
        self._error = None
        self._stop_event = Event()

    @property
    def coroutine(self):
        return self._coro

    @property
    def ident(self) -> int:
        return self._ident

    @property
    def is_daemon(self) -> bool:
        return self._daemon

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_alive(self):
        return self._state == TaskState.RUNNING

    async def _nio_on_start_(self, ident: int) -> None:
        self._ident = ident
        self._state = TaskState.RUNNING

    async def _nio_on_stop_(self, result, error: Exception) -> None:
        self._state = TaskState.STOPED
        self._result = result
        self._error = error
        await self._stop_event.notify()

    def __repr__(self):
        return '<Task#{} {}>'.format(self.ident, self.name)

    async def start(self):
        await nio_spawn(self)

    async def join(self):
        await self._stop_event.wait()
        if self._error is not None:
            raise self._error
        return self.result

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
def nio_spawn(task: Task) -> None:
    yield (nio_spawn, task)


@coroutine
def nio_current_task() -> Task:
    yield (nio_current_task,)


@coroutine
def nio_cancel(task: Task) -> None:
    yield (nio_cancel, task)


@coroutine
def nio_set_timeout(timeout: Timeout) -> None:
    yield (nio_set_timeout, timeout)


@coroutine
def nio_unset_timeout(timeout: Timeout) -> None:
    yield (nio_unset_timeout, timeout)


@coroutine
def nio_schedule_wait(event: Event) -> None:
    yield (nio_schedule_wait, event)


@coroutine
def nio_schedule_notify(event: Event) -> None:
    yield (nio_schedule_notify, event)
