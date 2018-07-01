import logging
from queue import Queue as ThreadQueue

from .error import ChannelClosed
from .newio_broker import NewioBroker
from .thread_broker import ThreadBroker

LOG = logging.getLogger(__name__)


class ThreadChannel:
    '''Message channel for communicating between threads and newio tasks'''

    def __init__(self, maxsize=0):
        self._queue = ThreadQueue(maxsize)
        self._thread_broker = ThreadBroker(self._queue)
        self._newio_broker = NewioBroker(self._queue)
        self._is_closed = False

    def __repr__(self):
        return f'<ThreadChannel at {hex(id(self))}>'

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('Channel already closed')
        LOG.debug('[starting] thread channel')
        await self._thread_broker.start()
        await self._newio_broker.start()
        LOG.debug('[started] thread channel')
        return self

    async def __aexit__(self, *exc_info):
        LOG.debug('[stopping] thread channel')
        self._is_closed = True
        await self._thread_broker.shutdown()
        await self._newio_broker.shutdown()
        LOG.debug('[stopped] thread channel')

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
        if self._is_closed:
            raise ChannelClosed()
        self._thread_broker.send(item)
        self._newio_broker.notify_recv()

    def thread_recv(self):
        '''recv in thread'''
        item = self._thread_broker.recv()
        self._newio_broker.notify_send()
        return item

    async def send(self, item):
        '''send in newio task'''
        if self._is_closed:
            raise ChannelClosed()
        await self._newio_broker.send(item)
        self._thread_broker.notify_recv()

    async def recv(self):
        '''recv in newio task'''
        item = await self._newio_broker.recv()
        self._thread_broker.notify_send()
        return item
