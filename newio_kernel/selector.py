import logging
from selectors import EVENT_READ, EVENT_WRITE
from selectors import DefaultSelector

LOG = logging.getLogger(__name__)


class KernelFd:

    def __init__(self, selector, user_fd, task):
        self._selector = selector
        self._nio_ref_ = user_fd
        user_fd._nio_ref_ = self
        self.task = task
        self.selector_key = None

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

    def fileno(self):
        return self._nio_ref_.fileno()

    def unregister(self):
        self._selector._unregister(self)

    def __repr__(self):
        return '<KernelFd#{} of task #{}>'.format(
            self.fileno(), self.task.ident)

    def clean(self):
        self.unregister()


class Selector:

    def __init__(self):
        self._sel = DefaultSelector()

    def register_read(self, task, user_fd):
        return self._register(task, user_fd, EVENT_READ)

    def register_write(self, task, user_fd):
        return self._register(task, user_fd, EVENT_WRITE)

    def _unregister(self, fd):
        if fd.selector_key is None:
            return
        LOG.debug('selector unregister fd %r', fd)
        fd.selector_key = None
        self._sel.unregister(fd)

    def _register(self, task, user_fd, events):
        fd = user_fd._nio_ref_
        if fd is None:
            fd = KernelFd(self, user_fd, task)
        if fd.task is not task:
            raise RuntimeError('file descriptor already waiting by other task')
        if fd.selector_key is None:
            LOG.debug('selector register fd %r, events=%r', fd, events)
            fd.selector_key = self._sel.register(fd, events)
        elif fd.events != events:
            LOG.debug('selector modify fd %r, events=%r', fd, events)
            fd.selector_key = self._sel.modify(fd, events)
        return fd

    def poll(self, timeout):
        io_events = self._sel.select(timeout=timeout)
        for key, mask in io_events:
            fd = key.fileobj
            if fd.task.is_alive and fd.events & mask:
                LOG.debug('task %r wakeup by selector fd %r', fd.task, fd)
                yield fd.task

    def close(self):
        self._sel.close()
