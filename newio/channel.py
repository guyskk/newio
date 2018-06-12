import logging
from queue import Queue as ThreadQueue
from queue import Empty as QUEUE_EMPTY
from queue import Full as QUEUE_FULL

from .api import spawn
from .socket import socketpair
from .sync import Condition

__all__ = ('Channel', 'ChannelClosed')

LOG = logging.getLogger(__name__)


class ChannelClosed(Exception):
    '''Exception raised when send or recv on closed channel'''


class Channel:
    '''Message channel for threads and coroutines'''

    def __init__(self, maxsize=0):
        self._notify_producer, self._notify_consumer = socketpair()
        self._queue = ThreadQueue(maxsize)
        self._send_waiting = Condition()
        self._recv_waiting = Condition()
        self._broker_waiting = Condition()
        self._num_consumer = 0
        self._num_producer = 0
        self._broker_task = None
        self._is_closed = False
        # a timeout to avoid threads block on closed channel
        self._blocking_timeout = 0.1

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('Channel already closed')
        self._broker_task = await spawn(self.channel_broker())
        return self

    async def __aexit__(self, *exc_info):
        self._is_closed = True
        await self._send_waiting.notify_all()
        await self._recv_waiting.notify_all()
        await self._broker_task.cancel()
        await self._notify_producer.close()
        await self._notify_consumer.close()

    async def __aiter__(self):
        while True:
            try:
                yield (await self.arecv())
            except ChannelClosed:
                break

    def __iter__(self):
        while True:
            try:
                yield self.recv()
            except ChannelClosed:
                break

    def send(self, item):
        '''send in thread'''
        while True:
            if self._is_closed:
                raise ChannelClosed()
            try:
                self._queue.put(item, timeout=self._blocking_timeout)
            except QUEUE_FULL:
                pass
            else:
                break
        if self._num_consumer > 0:
            LOG.debug('notify consumer on channel %r', self)
            with self._notify_consumer.blocking() as sock:
                sock.sendall(b'\1')

    def recv(self):
        '''recv in thread'''
        if self._is_closed:
            try:
                item = self._queue.get_nowait()
            except QUEUE_EMPTY:
                raise ChannelClosed() from None
        else:
            while True:
                if self._is_closed:
                    raise ChannelClosed()
                try:
                    item = self._queue.get(timeout=self._blocking_timeout)
                except QUEUE_EMPTY:
                    pass
                else:
                    break
        if self._num_producer > 0:
            LOG.debug('notify producer on channel %r', self)
            with self._notify_producer.blocking() as sock:
                sock.sendall(b'\1')
        return item

    async def asend(self, item):
        '''send in coroutine'''
        if self._is_closed:
            raise ChannelClosed()
        self._num_producer += 1
        try:
            while True:
                try:
                    self._queue.put_nowait(item)
                except QUEUE_FULL:
                    await self._broker_waiting.notify()
                    await self._send_waiting.wait()
                else:
                    break
        finally:
            self._num_producer -= 1

    async def arecv(self):
        '''recv in coroutine'''
        self._num_consumer += 1
        try:
            while True:
                try:
                    item = self._queue.get_nowait()
                except QUEUE_EMPTY:
                    if self._is_closed:
                        raise ChannelClosed() from None
                    await self._broker_waiting.notify()
                    await self._recv_waiting.wait()
                else:
                    break
        finally:
            self._num_consumer -= 1
        return item

    async def channel_broker(self):
        wakeup_consumer, wakeup_producer = \
            self._notify_producer, self._notify_consumer
        while True:
            if self._num_consumer > 0:
                LOG.debug('broker of channel %r wait wakeup_consumer', self)
                await wakeup_consumer.recv(1)
                await self._recv_waiting.notify()
            elif self._num_producer > 0:
                LOG.debug('broker of channel %r wait wakeup_producer', self)
                await wakeup_producer.recv(1)
                await self._send_waiting.notify()
            else:
                await self._broker_waiting.wait()
