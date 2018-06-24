import logging
from selectors import EVENT_READ, EVENT_WRITE
from selectors import DefaultSelector

LOG = logging.getLogger(__name__)


class KernelFd:

    def __init__(self, selector, fileno, task):
        self._selector = selector
        self._fileno = fileno
        self.task = task
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

    def register_read(self, task, fileno):
        return self._register(task, fileno, EVENT_READ)

    def register_write(self, task, fileno):
        return self._register(task, fileno, EVENT_WRITE)

    def _unregister(self, fd):
        fileno = fd.fileno()
        if fileno in self._fds:
            LOG.debug('unregister fd %r', fd)
            del self._fds[fileno]
            self._sel.unregister(fileno)

    def _register(self, task, fileno, events):
        fd = self._fds.get(fileno, None)
        if fd is None:
            fd = KernelFd(self, fileno, task)
            self._fds[fileno] = fd
        if fd.task is not task:
            raise RuntimeError(
                'file descriptor already registered by other task')
        if fd.selector_key is None:
            LOG.debug('register fd %r, events=%r', fd, events)
            fd.selector_key = self._sel.register(fd, events)
        elif fd.events != events:
            LOG.debug('modify fd %r, events=%r', fd, events)
            fd.selector_key = self._sel.modify(fd, events)
        return fd

    def poll(self, timeout):
        io_events = self._sel.select(timeout=timeout)
        for key, mask in io_events:
            fd = key.fileobj
            if fd.task.is_alive and fd.events & mask:
                LOG.debug('task %r wakeup by fd %r', fd.task, fd)
                yield fd.task

    def close(self):
        self._sel.close()
