from llist import dllist


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
        waiter = FutexWaiter(self, task)
        waiter.node = self._waiters.append(waiter)
        return waiter

    def _cancel_waiter(self, waiter):
        if not waiter.is_expired:
            self._waiters.remove(waiter.node)
            waiter.is_expired = True

    def wake_all(self):
        for waiter in self._waiters:
            task = waiter.task
            waiter.is_expired = True
            if task.is_alive:
                yield task
        self._waiters.clear()

    def wake(self, n):
        while self._waiters and n > 0:
            waiter = self.popleft()
            waiter.is_expired = True
            task = waiter.task
            if task.is_alive:
                yield task
                n -= 1

    def __repr__(self):
        num = len(self._waiters)
        return f'<KernelFutex@{hex(id(self._nio_ref_))} {num} waiters>'


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
