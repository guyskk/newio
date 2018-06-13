'''synchronization primitives'''
from .syscall import Lounge


class BrokenBarrierError(RuntimeError):
    '''Exception raised if the barrier is broken'''


class Lock:
    def __init__(self):
        self._is_locked = False
        self._lounge = Lounge()

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, *exc_info):
        await self.release()

    async def acquire(self):
        if self._is_locked:
            await self._lounge.wait()
        self._is_locked = True

    async def release(self):
        if not self._is_locked:
            raise RuntimeError('release unlocked lock')
        self._is_locked = False
        await self._lounge.wake(1)

    def locked(self):
        return self._is_locked


class Condition:
    def __init__(self):
        self._lounge = Lounge()

    async def wait(self):
        await self._lounge.wait()

    async def notify(self, n=1):
        await self._lounge.wake(n)

    async def notify_all(self):
        return await self.notify(Lounge.WAKE_ALL)


class Semaphore:
    def __init__(self, value=1):
        if value < 0:
            raise ValueError('semaphore initial value must be >= 0')
        self._value = value
        self._lounge = Lounge()

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, *exc_info):
        await self.release()

    async def acquire(self):
        if self._value <= 0:
            await self._lounge.wait()
        self._value -= 1

    async def release(self):
        self._value += 1
        if self._value == 1:
            await self._lounge.wake(1)


class BoundedSemaphore:
    def __init__(self, value=1):
        if value < 0:
            raise ValueError('semaphore initial value must be >= 0')
        self._value = value
        self._init_value = value
        self._lounge = Lounge()

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, *exc_info):
        await self.release()

    async def acquire(self):
        if self._value <= 0:
            await self._lounge.wait()
        self._value -= 1

    async def release(self):
        if self._value >= self._init_value:
            raise RuntimeError('semaphore released too many times')
        self._value += 1
        if self._value == 1:
            await self._lounge.wake(1)


class Event:
    def __init__(self):
        self._lounge = Lounge()
        self._is_set = False

    def is_set(self):
        return self._is_set

    async def set(self):
        if self._is_set:
            return
        self._is_set = True
        await self._lounge.wake(Lounge.WAKE_ALL)

    def clear(self):
        self._is_set = False

    async def wait(self):
        if self._is_set:
            return
        await self._lounge.wait()


class Barrier:
    def __init__(self, parties, action=None):
        self._lounge = Lounge()
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
                await self._lounge.wake(Lounge.WAKE_ALL)
                if self._action is not None:
                    self._action()
            else:
                await self._lounge.wait()
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
            await self._lounge.wake(Lounge.WAKE_ALL)
