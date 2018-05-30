from llist import dllist


class KernelFutex:

    def __init__(self, user_futex):
        self._nio_ref_ = user_futex
        user_futex._nio_ref_ = self
        self._tasks = dllist()

    @staticmethod
    def of(user_futex):
        futex = user_futex._nio_ref_
        if futex is None:
            futex = KernelFutex(user_futex)
        return futex

    def add_waiter(self, task):
        node = self._tasks.append(task)
        return FutexWaiter(self, task, node)

    def _cancel_waiter(self, waiter):
        return  # TODO
        if not waiter.cancelled:
            self._tasks.remove(waiter.node)
            waiter.cancelled = True

    def wake_all(self):
        for task in self._tasks:
            if task.is_alive:
                yield task
        self._tasks.clear()

    def wake(self, n):
        while self._tasks and n > 0:
            task = self.popleft()
            if task.is_alive:
                yield task
                n -= 1

    def __repr__(self):
        num = len(self._tasks)
        return f'<KernelFutex@{hex(id(self._nio_ref_))} {num} waiters>'


class FutexWaiter:
    def __init__(self, futex, task, node):
        self.futex = futex
        self.task = task
        self.node = node
        self.cancelled = False

    def cancel(self):
        self.futex._cancel_waiter(self)
