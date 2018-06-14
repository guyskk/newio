import logging
import asyncio
from queue import Queue as ThreadQueue
from queue import Empty as QUEUE_EMPTY
from queue import Full as QUEUE_FULL

from newio.api import spawn, run_in_asyncio
from newio.socket import socketpair
from newio.sync import Condition

from .error import ChannelClosed

LOG = logging.getLogger(__name__)


async def cond_wait(cond):
    await cond.acquire()
    try:
        await cond.wait()
    finally:
        cond.release()


async def cond_notify(cond):
    await cond.acquire()
    try:
        cond.notify()
    finally:
        cond.release()


class AsyncioChannel:
    '''Message channel for communicating between asyncio tasks and newio tasks'''

    def __init__(self, maxsize=0):
        self._notify_producer, self._notify_consumer = socketpair()
        self._wakeup_consumer, self._wakeup_producer = \
            self._notify_producer, self._notify_consumer
        self._queue = ThreadQueue(maxsize)
        self._asyncio_send_waiting = None
        self._asyncio_recv_waiting = None
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

    async def _init_asyncio_waiting(self):

        async def _init():
            self._asyncio_send_waiting = asyncio.Condition()
            self._asyncio_recv_waiting = asyncio.Condition()

        await run_in_asyncio(_init())

    def __repr__(self):
        return f'<AsyncioChannel at {hex(id(self))}>'

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('Channel already closed')
        await self._init_asyncio_waiting()
        self._consumer_broker = await spawn(self.consumer_broker())
        self._producer_broker = await spawn(self.producer_broker())
        self._asyncio_consumer_broker = \
            await spawn(self.asyncio_consumer_broker())
        self._asyncio_producer_broker = \
            await spawn(self.asyncio_producer_broker())
        return self

    async def __aexit__(self, *exc_info):
        self._is_closed = True
        await self._send_waiting.notify_all()
        await self._recv_waiting.notify_all()
        self._notify_producer.socket.sendall(b'yx')
        self._notify_consumer.socket.sendall(b'yx')
        await self._consumer_broker.join()
        await self._producer_broker.join()
        await self._asyncio_consumer_broker.join()
        await self._asyncio_producer_broker.join()

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
                    await cond_wait(self._asyncio_send_waiting)
                else:
                    break
        finally:
            self._num_asyncio_producer -= 1
        if self._num_consumer > 0:
            LOG.debug('notify consumer on channel %r', self)
            loop = asyncio.get_event_loop()
            await loop.sock_sendall(self._notify_consumer.socket, b'1')
        if self._num_asyncio_consumer > 0:
            LOG.debug('notify asyncio consumer on channel %r', self)
            await cond_notify(self._asyncio_recv_waiting)

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
                    await cond_wait(self._asyncio_recv_waiting)
                else:
                    break
        finally:
            self._num_asyncio_consumer -= 1
        if self._num_producer > 0:
            LOG.debug('notify producer on channel %r', self)
            loop = asyncio.get_event_loop()
            await loop.sock_sendall(self._notify_producer.socket, b'1')
        if self._num_asyncio_producer > 0:
            LOG.debug('notify asyncio producer on channel %r', self)
            await cond_notify(self._asyncio_send_waiting)
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
            await self._notify_consumer.sendall(b'1')
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
                except QUEUE_EMPTY:
                    if self._is_closed:
                        raise ChannelClosed() from None
                    await self._recv_waiting.wait()
                else:
                    break
        finally:
            self._num_consumer -= 1
        if self._num_asyncio_producer > 0:
            LOG.debug('notify asyncio producer on channel %r', self)
            await self._notify_producer.sendall(b'1')
        if self._num_producer > 0:
            LOG.debug('notify producer on channel %r', self)
            await self._send_waiting.notify()
        return item

    async def asyncio_consumer_broker(self):

        async def _consumer_broker():
            LOG.debug('asyncio consumer broker started')
            loop = asyncio.get_event_loop()
            sock = self._wakeup_consumer.socket
            while True:
                nbytes = await loop.sock_recv(sock, 1)
                if not nbytes or nbytes == b'y' or nbytes == b'x':
                    LOG.debug('asyncio consumer broker exiting')
                    if nbytes == b'x':
                        sock.close()
                    break
                await cond_notify(self._asyncio_recv_waiting)

        await run_in_asyncio(_consumer_broker())

    async def asyncio_producer_broker(self):

        async def _producer_broker():
            LOG.debug('asyncio producer broker started')
            loop = asyncio.get_event_loop()
            sock = self._wakeup_producer.socket
            while True:
                nbytes = await loop.sock_recv(sock, 1)
                if not nbytes or nbytes == b'y' or nbytes == b'x':
                    LOG.debug('asyncio producer broker exiting')
                    if nbytes == b'x':
                        sock.close()
                    break
                await cond_notify(self._asyncio_send_waiting)

        await run_in_asyncio(_producer_broker())

    async def consumer_broker(self):
        LOG.debug('consumer broker started')
        while True:
            nbytes = await self._wakeup_consumer.recv(1)
            if not nbytes or nbytes == b'y' or nbytes == b'x':
                if nbytes == b'x':
                    LOG.debug('consumer broker exiting')
                    await self._wakeup_consumer.close()
                break
            await self._recv_waiting.notify()

    async def producer_broker(self):
        LOG.debug('producer broker started')
        while True:
            nbytes = await self._wakeup_producer.recv(1)
            if not nbytes or nbytes == b'y' or nbytes == b'x':
                if nbytes == b'x':
                    LOG.debug('producer broker exiting')
                    await self._wakeup_producer.close()
                break
            await self._send_waiting.notify()
