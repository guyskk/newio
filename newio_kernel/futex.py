import logging
from llist import dllist

LOG = logging.getLogger(__name__)


class KernelFutex:

    def __init__(self, user_futex):
        self._nio_ref_ = user_futex
        user_futex._nio_ref_ = self
        self._waiters = dllist()

    @staticmethod
    def of(user_futex):
        futex = user_futex._nio_ref_
        if futex is None:
            futex = KernelFutex(user_futex)
        return futex

    def add_waiter(self, task):
        LOG.debug('task %r waiting for futex %r', task, self)
        waiter = FutexWaiter(self, task)
        waiter.node = self._waiters.append(waiter)
        return waiter

    def _cancel_waiter(self, waiter):
        if not waiter.is_expired:
            LOG.debug('task %r cancel waiting futex %r', waiter.task, self)
            self._waiters.remove(waiter.node)
            waiter.is_expired = True

    def wake_all(self):
        for waiter in self._waiters:
            task = waiter.task
            waiter.is_expired = True
            if task.is_alive:
                LOG.debug('task %r wakeup by futex %r', task, self)
                yield task
        self._waiters.clear()

    def wake(self, n):
        while self._waiters and n > 0:
            waiter = self._waiters.popleft()
            waiter.is_expired = True
            task = waiter.task
            if task.is_alive:
                LOG.debug('task %r wakeup by futex %r', task, self)
                yield task
                n -= 1

    def __repr__(self):
        return f'<KernelFutex@{hex(id(self._nio_ref_))}>'


class FutexWaiter:
    def __init__(self, futex, task):
        self.futex = futex
        self.task = task
        self.node = None
        self.is_expired = False

    def cancel(self):
        self.futex._cancel_waiter(self)

    def state(self):
        return 'wait_futex'

    def clean(self):
        self.cancel()
