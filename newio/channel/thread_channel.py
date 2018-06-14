import logging
from queue import Queue as ThreadQueue
from queue import Empty, Full

from newio.api import spawn
from newio.socket import socketpair
from newio.sync import Condition

from .error import ChannelClosed

LOG = logging.getLogger(__name__)


class ThreadChannel:
    '''Message channel for communicating between threads and newio tasks'''

    def __init__(self, maxsize=0):
        self._notify_producer, self._notify_consumer = socketpair()
        self._wakeup_consumer, self._wakeup_producer = \
            self._notify_producer, self._notify_consumer
        self._queue = ThreadQueue(maxsize)
        self._send_waiting = Condition()
        self._recv_waiting = Condition()
        self._num_consumer = 0
        self._num_producer = 0
        self._is_closed = False
        # a timeout to avoid threads block on closed channel
        self._blocking_timeout = 0.1
        self._consumer_broker = None
        self._producer_broker = None

    def __repr__(self):
        return f'<ThreadChannel at {hex(id(self))}>'

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('Channel already closed')
        self._consumer_broker = await spawn(self.consumer_broker())
        self._producer_broker = await spawn(self.producer_broker())
        return self

    async def __aexit__(self, *exc_info):
        self._is_closed = True
        await self._send_waiting.notify_all()
        await self._recv_waiting.notify_all()
        self._notify_producer.socket.sendall(b'x')
        self._notify_consumer.socket.sendall(b'x')
        await self._consumer_broker.join()
        await self._producer_broker.join()

    async def __aiter__(self):
        while True:
            try:
                yield (await self.recv())
            except ChannelClosed:
                break

    def thread_iter(self):
        while True:
            try:
                yield self.thread_recv()
            except ChannelClosed:
                break

    def thread_send(self, item):
        '''send in thread'''
        while True:
            if self._is_closed:
                raise ChannelClosed()
            try:
                self._queue.put(item, timeout=self._blocking_timeout)
            except Full:
                pass
            else:
                break
        if self._num_consumer > 0:
            LOG.debug('notify consumer on channel %r', self)
            with self._notify_consumer.blocking() as sock:
                sock.sendall(b'1')

    def thread_recv(self):
        '''recv in thread'''
        if self._is_closed:
            try:
                return self._queue.get_nowait()
            except Empty:
                raise ChannelClosed() from None
        while True:
            if self._is_closed:
                raise ChannelClosed()
            try:
                item = self._queue.get(timeout=self._blocking_timeout)
            except Empty:
                pass
            else:
                break
        if self._num_producer > 0:
            LOG.debug('notify producer on channel %r', self)
            with self._notify_producer.blocking() as sock:
                sock.sendall(b'1')
        return item

    async def send(self, item):
        '''send in newio task'''
        if self._is_closed:
            raise ChannelClosed()
        self._num_producer += 1
        try:
            while True:
                try:
                    self._queue.put_nowait(item)
                except Full:
                    await self._send_waiting.wait()
                else:
                    break
        finally:
            self._num_producer -= 1
        if self._num_consumer > 0:
            LOG.debug('notify consumer on channel %r', self)
            await self._recv_waiting.notify()

    async def recv(self):
        '''recv in newio task'''
        self._num_consumer += 1
        try:
            while True:
                try:
                    item = self._queue.get_nowait()
                except Empty:
                    if self._is_closed:
                        raise ChannelClosed() from None
                    await self._recv_waiting.wait()
                else:
                    break
        finally:
            self._num_consumer -= 1
        if self._num_producer > 0:
            LOG.debug('notify producer on channel %r', self)
            await self._send_waiting.notify()
        return item

    async def consumer_broker(self):
        LOG.debug('consumer broker started')
        while True:
            nbytes = await self._wakeup_consumer.recv(1)
            if not nbytes or nbytes == b'x':
                LOG.debug('consumer broker exiting')
                await self._wakeup_consumer.close()
                break
            await self._recv_waiting.notify()

    async def producer_broker(self):
        LOG.debug('producer broker started')
        while True:
            nbytes = await self._wakeup_producer.recv(1)
            if not nbytes or nbytes == b'x':
                LOG.debug('producer broker exiting')
                await self._wakeup_producer.close()
                break
            await self._send_waiting.notify()
