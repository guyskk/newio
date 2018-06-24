import logging

LOG = logging.getLogger(__name__)


class KernelLounge:

    def __init__(self, user_lounge):
        self._nio_ref_ = user_lounge
        user_lounge._nio_ref_ = self
        self._waiters = set()

    @property
    def ident(self):
        return self._nio_ref_.ident

    @staticmethod
    def of(user_lounge):
        lounge = user_lounge._nio_ref_
        if lounge is None:
            lounge = KernelLounge(user_lounge)
        return lounge

    def add_waiter(self, task):
        LOG.debug('add task %r to lounge %r', task, self)
        waiter = LoungeWaiter(self, task)
        self._waiters.add(waiter)
        return waiter

    def _cancel_waiter(self, waiter):
        if not waiter.is_expired:
            LOG.debug('task %r cancel waiting lounge %r', waiter.task, self)
            self._waiters.remove(waiter)
            waiter.is_expired = True

    def wake_all(self):
        waiters = self._waiters
        self._waiters = set()
        for waiter in waiters:
            task = waiter.task
            waiter.is_expired = True
            if task.is_alive:
                LOG.debug('wakeup task %r on lounge %r', task, self)
                yield task

    def wake(self, n):
        while self._waiters and n > 0:
            waiter = self._waiters.pop()
            waiter.is_expired = True
            task = waiter.task
            if task.is_alive:
                LOG.debug('task %r wakeup by lounge %r', task, self)
                yield task
                n -= 1

    def __repr__(self):
        return f'<KernelLounge@{hex(self.ident)[2:]}>'


class LoungeWaiter:
    def __init__(self, lounge, task):
        self.lounge = lounge
        self.task = task
        self.is_expired = False

    def cancel(self):
        self.lounge._cancel_waiter(self)

    def state(self):
        return 'wait_lounge'

    def clean(self):
        self.cancel()
