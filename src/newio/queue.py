from queue import Empty, Full
from collections import deque
from heapq import heappush, heappop

from .sync import Condition

__all__ = ('Queue', 'PriorityQueue', 'LifoQueue')


class Queue:
    '''
    A queue for communicating between tasks.
    '''

    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self._get_waiting = Condition()
        self._put_waiting = Condition()
        self._join_waiting = Condition()
        self._task_count = 0
        self._queue = self._init_internal_queue()

    def __repr__(self):
        name = type(self).__name__
        return f'<{name}, size={self.qsize()}>'

    def _init_internal_queue(self):
        return deque()

    def qsize(self):
        return len(self._queue)

    def empty(self):
        return not self._queue

    def full(self):
        if self.maxsize is None:
            return False
        return self.qsize() >= self.maxsize

    async def get(self):
        while self.empty():
            await self._get_waiting.wait()
        return await self.get_nowait()

    async def get_nowait(self):
        if self.empty():
            raise Empty()
        result = self._get()
        await self._put_waiting.notify()
        return result

    def _get(self):
        return self._queue.popleft()

    async def join(self):
        if self._task_count > 0:
            await self._join_waiting.wait()

    async def put(self, item):
        while self.full():
            await self._put_waiting.wait()
        await self.put_nowait(item)

    async def put_nowait(self, item):
        if self.full():
            raise Full()
        self._put(item)
        self._task_count += 1
        await self._get_waiting.notify()

    def _put(self, item):
        self._queue.append(item)

    async def task_done(self):
        self._task_count -= 1
        if self._task_count <= 0:
            await self._join_waiting.notify_all()


class PriorityQueue(Queue):
    '''
    A Queue that outputs an item with the lowest priority first

    Items have to be orderable objects
    '''

    def _init_internal_queue(self):
        return []

    def _put(self, item):
        heappush(self._queue, item)

    def _get(self):
        return heappop(self._queue)


class LifoQueue(Queue):
    '''
    Last In First Out queue

    Retrieves most recently added items first
    '''

    def _init_internal_queue(self):
        return []

    def _put(self, item):
        self._queue.append(item)

    def _get(self):
        return self._queue.pop()
