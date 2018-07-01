import logging
from queue import Queue as ThreadQueue

from .error import ChannelClosed
from .asyncio_broker import AsyncioBroker
from .newio_broker import NewioBroker

LOG = logging.getLogger(__name__)


class AsyncioChannel:
    '''Message channel for communicating between asyncio tasks and newio tasks'''

    def __init__(self, maxsize=0):
        self._queue = ThreadQueue(maxsize)
        self._asyncio_broker = AsyncioBroker(self._queue)
        self._newio_broker = NewioBroker(self._queue)
        self._is_closed = False

    def __repr__(self):
        return f'<ThreadChannel at {hex(id(self))}>'

    async def __aenter__(self):
        if self._is_closed:
            raise RuntimeError('Channel already closed')
        LOG.debug('[starting] asyncio channel')
        await self._asyncio_broker.start()
        await self._newio_broker.start()
        LOG.debug('[started] asyncio channel')
        return self

    async def __aexit__(self, *exc_info):
        LOG.debug('[stopping] asyncio channel')
        self._is_closed = True
        await self._asyncio_broker.shutdown()
        await self._newio_broker.shutdown()
        LOG.debug('[stopped] asyncio channel')

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
        '''send in asyncio'''
        if self._is_closed:
            raise ChannelClosed()
        await self._asyncio_broker.send(item)
        self._newio_broker.notify_recv()

    async def asyncio_recv(self):
        '''recv in asyncio'''
        item = await self._asyncio_broker.recv()
        self._newio_broker.notify_send()
        return item

    async def send(self, item):
        '''send in newio task'''
        if self._is_closed:
            raise ChannelClosed()
        await self._newio_broker.send(item)
        self._asyncio_broker.notify_recv()

    async def recv(self):
        '''recv in newio task'''
        item = await self._newio_broker.recv()
        self._asyncio_broker.notify_send()
        return item
