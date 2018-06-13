import logging
import asyncio
from queue import Queue as ThreadQueue
from queue import Empty as QUEUE_EMPTY
from queue import Full as QUEUE_FULL

from .api import spawn
from .socket import socketpair
from .sync import Condition

__all__ = ('ThreadChannel', 'AsyncioChannel', 'ChannelClosed')

LOG = logging.getLogger(__name__)


class ChannelClosed(Exception):
    '''Exception raised when send or recv on closed channel'''


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

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('Channel already closed')
        self._consumer_broker = await spawn(self.channel_consumer_broker())
        self._producer_broker = await spawn(self.channel_producer_broker())
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
            except QUEUE_FULL:
                pass
            else:
                break
        if self._num_consumer > 0:
            LOG.debug('notify newio consumer on channel %r', self)
            with self._notify_consumer.blocking() as sock:
                sock.sendall(b'\1')

    def thread_recv(self):
        '''recv in thread'''
        if self._is_closed:
            try:
                return self._queue.get_nowait()
            except QUEUE_EMPTY:
                raise ChannelClosed() from None
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
            LOG.debug('notify newio producer on channel %r', self)
            with self._notify_producer.blocking() as sock:
                sock.sendall(b'\1')
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
                except QUEUE_FULL:
                    await self._send_waiting.wait()
                else:
                    break
        finally:
            self._num_producer -= 1
        if self._num_consumer > 0:
            LOG.debug('notify newio consumer on channel %r', self)
            await self._recv_waiting.notify()

    async def recv(self):
        '''recv in newio task'''
        self._num_consumer += 1
        try:
            while True:
                try:
                    item = self._queue.get_nowait()
                except QUEUE_EMPTY:
                    if self._is_closed:
                        raise ChannelClosed() from None
                    await self._recv_waiting.wait()
                else:
                    break
        finally:
            self._num_consumer -= 1
        if self._num_producer > 0:
            LOG.debug('notify newio producer on channel %r', self)
            await self._send_waiting.notify()
        return item

    async def channel_consumer_broker(self):
        while True:
            nbytes = await self._wakeup_consumer.recv(1)
            if not nbytes or nbytes == b'x':
                LOG.debug('channel_consumer_broker exiting')
                await self._wakeup_consumer.close()
                break
            await self._recv_waiting.notify(len(nbytes))

    async def channel_producer_broker(self):
        while True:
            nbytes = await self._wakeup_producer.recv(1)
            if not nbytes or nbytes == b'x':
                LOG.debug('channel_producer_broker exiting')
                await self._wakeup_producer.close()
                break
            await self._send_waiting.notify(len(nbytes))


class AsyncioChannel:
    '''Message channel for communicating between asyncio tasks and newio tasks'''

    def __init__(self, maxsize=0):
        self._notify_producer, self._notify_consumer = socketpair()
        self._wakeup_consumer, self._wakeup_producer = \
            self._notify_producer, self._notify_consumer
        self._queue = ThreadQueue(maxsize)
        self._asyncio_send_waiting = asyncio.Condition()
        self._asyncio_recv_waiting = asyncio.Condition()
        self._num_asyncio_consumer = 0
        self._num_asyncio_producer = 0
        self._send_waiting = Condition()
        self._recv_waiting = Condition()
        self._num_consumer = 0
        self._num_producer = 0
        self._is_closed = False
        self._consumer_broker = None
        self._producer_broker = None
        self._asyncio_consumer_broker = None
        self._asyncio_producer_broker = None

    @property
    def loop(self):
        return asyncio.get_event_loop()

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('Channel already closed')
        self._consumer_broker = await spawn(self.channel_consumer_broker())
        self._producer_broker = await spawn(self.channel_producer_broker())
        self._asyncio_consumer_broker = \
            self.loop.create_task(self.asyncio_channel_consumer_broker())
        self._asyncio_producer_broker = \
            self.loop.create_task(self.asyncio_channel_producer_broker())
        return self

    async def __aexit__(self, *exc_info):
        self._is_closed = True
        await self._send_waiting.notify_all()
        await self._recv_waiting.notify_all()
        self._notify_producer.socket.sendall(b'yx')
        self._notify_consumer.socket.sendall(b'yx')
        await self._consumer_broker.join()
        await self._producer_broker.join()
        # TODO: join asyncio tasks

    async def __aiter__(self):
        while True:
            try:
                yield (await self.recv())
            except ChannelClosed:
                break

    async def asyncio_iter(self):
        while True:
            try:
                yield (await self.asyncio_recv())
            except ChannelClosed:
                break

    async def asyncio_send(self, item):
        '''send in asyncio task'''
        if self._is_closed:
            raise ChannelClosed()
        self._num_asyncio_producer += 1
        try:
            while True:
                try:
                    self._queue.put_nowait(item)
                except QUEUE_FULL:
                    await self._asyncio_send_waiting.wait()
                else:
                    break
        finally:
            self._num_asyncio_producer -= 1
        if self._num_consumer > 0:
            LOG.debug('notify newio consumer on channel %r', self)
            await self.loop.sock_sendall(self._notify_consumer.socket, b'\1')
        if self._num_asyncio_consumer > 0:
            LOG.debug('notify asyncio consumer on channel %r', self)
            await self._asyncio_recv_waiting.notify()

    async def asyncio_recv(self):
        '''recv in asyncio task'''
        self._num_asyncio_consumer += 1
        try:
            while True:
                try:
                    item = self._queue.get_nowait()
                except QUEUE_EMPTY:
                    if self._is_closed:
                        raise ChannelClosed() from None
                    await self._asyncio_recv_waiting.wait()
                else:
                    break
        finally:
            self._num_asyncio_consumer -= 1
        if self._num_producer > 0:
            LOG.debug('notify newio producer on channel %r', self)
            await self.loop.sock_sendall(self._notify_producer.socket, b'\1')
        if self._num_asyncio_producer > 0:
            LOG.debug('notify asyncio producer on channel %r', self)
            await self._asyncio_send_waiting.notify()
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
                except QUEUE_FULL:
                    await self._send_waiting.wait()
                else:
                    break
        finally:
            self._num_producer -= 1
        if self._num_asyncio_consumer > 0:
            LOG.debug('notify asyncio consumer on channel %r', self)
            await self._notify_consumer.sendall(b'\1')
        if self._notify_consumer > 0:
            LOG.debug('notify newio consumer on channel %r', self)
            await self._recv_waiting.notify()

    async def recv(self):
        '''recv in newio task'''
        self._num_consumer += 1
        try:
            while True:
                try:
                    item = self._queue.get_nowait()
                except QUEUE_EMPTY:
                    if self._is_closed:
                        raise ChannelClosed() from None
                    await self._recv_waiting.wait()
                else:
                    break
        finally:
            self._num_consumer -= 1
        if self._num_asyncio_producer > 0:
            await self._notify_producer.sendall(b'\1')
        if self._notify_producer > 0:
            LOG.debug('notify newio producer on channel %r', self)
            await self._send_waiting.notify()
        return item

    async def asyncio_channel_consumer_broker(self):
        sock = self._wakeup_consumer.socket
        while True:
            nbytes = await self.loop.sock_recv(sock, 1)
            if not nbytes or nbytes == b'y' or nbytes == b'x':
                LOG.debug('asyncio_channel_consumer_broker exiting')
                if nbytes == b'x':
                    sock.close()
                break
            await self._asyncio_recv_waiting.notify(len(nbytes))

    async def asyncio_channel_producer_broker(self):
        sock = self._wakeup_producer.socket
        while True:
            nbytes = await self.loop.sock_recv(sock, 1)
            if not nbytes or nbytes == b'y' or nbytes == b'x':
                LOG.debug('asyncio_channel_producer_broker exiting')
                if nbytes == b'x':
                    sock.close()
                break
            await self._asyncio_send_waiting.notify(len(nbytes))

    async def channel_consumer_broker(self):
        while True:
            nbytes = await self._wakeup_consumer.recv(1)
            if not nbytes or nbytes == b'y' or nbytes == b'x':
                if nbytes == b'x':
                    await self._wakeup_consumer.close()
                break
            await self._recv_waiting.notify(len(nbytes))

    async def channel_producer_broker(self):
        while True:
            nbytes = await self._wakeup_producer.recv(1)
            if not nbytes or nbytes == b'y' or nbytes == b'x':
                if nbytes == b'x':
                    await self._wakeup_producer.close()
                break
            await self._send_waiting.notify(len(nbytes))
