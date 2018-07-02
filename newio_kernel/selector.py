import logging
from selectors import EVENT_READ, EVENT_WRITE
from selectors import DefaultSelector

LOG = logging.getLogger(__name__)


class KernelFd:

    def __init__(self, selector, fileno, task):
        self._selector = selector
        self._fileno = fileno
        self.task = task
        self.idle = False
        self.taker = None
        self.taker_events = 0
        self.selector_key = None

    def fileno(self):
        return self._fileno

    def state(self):
        if self.events == EVENT_READ:
            return 'wait_read'
        elif self.events == EVENT_WRITE:
            return 'wait_write'
        else:
            return 'wait_io'

    @property
    def events(self):
        if self.selector_key is None:
            return 0
        return self.selector_key.events

    def unregister(self):
        self._selector._unregister(self)

    def __repr__(self):
        return (f'<KernelFd#{self.fileno()} of '
                f'task #{self.task.ident} {self.task.name}>')

    def clean(self):
        self.unregister()


class Selector:

    def __init__(self):
        self._sel = DefaultSelector()
        self._fds = {}
        self._idles = set()

    def register_read(self, task, fileno):
        return self._register(task, fileno, EVENT_READ)

    def register_write(self, task, fileno):
        return self._register(task, fileno, EVENT_WRITE)

    def _unregister(self, fd):
        LOG.debug('release fd %r', fd)
        fd.idle = True
        self._idles.add(fd)

    def _register(self, task, fileno, events):
        fd = self._fds.get(fileno, None)
        if fd is None:
            fd = KernelFd(self, fileno, task)
            self._fds[fileno] = fd
            LOG.debug('register fd %r, events=%r', fd, events)
            fd.selector_key = self._sel.register(fd, events)
            return fd
        if fd.idle and fd.taker is None:
            fd.taker = task
            fd.taker_events = events
            return fd
        if fd.task is not task:
            raise RuntimeError(f'file descriptor {fd!r} already registered')
        return fd

    def poll(self, timeout):
        io_events = self._sel.select(timeout=timeout)
        for key, mask in io_events:
            fd = key.fileobj
            if fd.task.is_alive:
                LOG.debug('task %r wakeup by fd %r', fd.task, fd)
                yield fd.task

    def flush(self):
        while self._idles:
            fd = self._idles.pop()
            if fd.taker is None:
                LOG.debug('unregister fd %r', fd)
                fileno = fd.fileno()
                del self._fds[fileno]
                self._sel.unregister(fileno)
            else:
                if fd.taker_events != fd.events:
                    LOG.debug('modify fd %r, events=%r', fd, fd.taker_events)
                    fd.selector_key = self._sel.modify(fd, fd.taker_events)
                if fd.task != fd.taker:
                    LOG.debug('take over fd %r, taker=%r', fd, fd.taker)
                    fd.task = fd.taker
                else:
                    LOG.debug('reuse fd %r, task=%r', fd, fd.task)
            fd.idle = False
            fd.taker = None
            fd.taker_events = 0

    def close(self):
        self._sel.close()
