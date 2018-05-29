'''synchronization primitives'''
from . import syscall
from .syscall import Futex


class Lock:
    def __init__(self):
        self._is_locked = False
        self._futex = Futex()

    async def acquire(self):
        if self._is_locked:
            await syscall.nio_futex_wait(self._futex)
        self._is_locked = True

    async def release(self):
        if not self._is_locked:
            raise RuntimeError('release unlocked lock')
        self._is_locked = False
        await syscall.nio_futex_wake(self._futex, 1)

    def locked(self):
        return self._is_locked


class Condition:
    def __init__(self):
        self._futex = Futex()

    async def wait(self):
        await syscall.nio_futex_wait(self._futex)

    async def notify(self, n=1):
        await syscall.nio_futex_wake(self._futex, n)

    async def notify_all(self):
        return self.notify(Futex.WAKE_ALL)


class Semaphore:
    def __init__(self, value=1):
        if value < 0:
            raise ValueError('semaphore initial value must be >= 0')
        self._value = value
        self._futex = Futex()

    async def acquire(self):
        if self._value <= 0:
            await syscall.nio_futex_wait(self._futex)
        self._value -= 1

    async def release(self):
        self._value += 1
        if self._value == 1:
            await syscall.nio_futex_wake(self._futex, 1)


class BoundedSemaphore:
    def __init__(self, value=1):
        if value < 0:
            raise ValueError('semaphore initial value must be >= 0')
        self._value = value
        self._init_value = value
        self._futex = Futex()

    async def acquire(self):
        if self._value <= 0:
            await syscall.nio_futex_wait(self._futex)
        self._value -= 1

    async def release(self):
        if self._value >= self._init_value:
            raise ValueError('semaphore released too many times')
        self._value += 1
        if self._value == 1:
            await syscall.nio_futex_wake(self._futex, 1)


class Event:
    def __init__(self):
        self._futex = Futex()
        self._is_set = False

    def is_set(self):
        return self._is_set

    async def set(self):
        if self._is_set:
            return
        self._is_set = True
        await self.nio_futex_wake(self._futex, Futex.WAKE_ALL)

    def clear(self):
        self._is_set = False

    async def wait(self):
        if self._is_set:
            return
        await self.nio_futex_wait(self._futex)


class BrokenBarrierError(Exception):
    pass


class Barrier:
    def __init__(self, parties, action=None):
        self._futex = Futex()
        self._parties = parties
        self._action = action
        self._count = 0
        self._is_broken = False
        self._is_filled = False

    async def wait(self):
        if self._is_broken:
            raise BrokenBarrierError()
        if self._is_filled:
            return 0
        index = self._count
        self._count += 1
        try:
            if self._count >= self._parties:
                self._is_filled = True
                await self.nio_futex_wake(self._futex, Futex.WAKE_ALL)
                if self._action is not None:
                    self._action()
            else:
                await self.nio_futex_wait(self._futex)
        except BaseException:
            self._is_broken = True
            raise
        finally:
            self._count -= 1
        if self._is_broken:
            raise BrokenBarrierError()
        return index

    # TODO: implement barrier reset
    # async def reset(self):
    #    pass

    async def abort(self):
        self._is_broken = True
        if self._count > 0:
            await self.nio_futex_wake(self._futex, Futex.WAKE_ALL)
