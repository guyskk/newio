import logging

from .error import ChannelClosed
from .controller import ChannelController
from .newio_broker import NewioBroker
from .thread_broker import ThreadBroker
from .asyncio_broker import AsyncioBroker


LOG = logging.getLogger(__name__)


class Channel:
    """Message channel for communicating between threads/newio/asyncio"""

    def __init__(self, bufsize=None):
        self.controller = ChannelController(bufsize)
        self._thread_broker = ThreadBroker(self.controller)
        self._newio_broker = NewioBroker(self.controller)
        self._asyncio_broker = AsyncioBroker(self.controller)

    def close(self):
        self.controller.close()

    async def __aenter__(self):
        if self.controller.closed:
            raise RuntimeError('Channel already closed')
        return self

    async def __aexit__(self, *exc_info):
        self.close()
        await self._thread_broker.join()
        await self._newio_broker.join()
        await self._asyncio_broker.join()
        self.controller.destroy()

    async def send(self, item):
        """send in newio task"""
        await self._newio_broker.send(item)

    async def recv(self):
        """recv in newio task"""
        return await self._newio_broker.recv()

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
        """send in asyncio"""
        await self._asyncio_broker.send(item)

    async def asyncio_recv(self):
        """recv in asyncio"""
        return await self._asyncio_broker.recv()

    def thread_iter(self):
        while True:
            try:
                yield self.thread_recv()
            except ChannelClosed:
                break

    def thread_send(self, item):
        """send in thread"""
        self._thread_broker.send(item)

    def thread_recv(self):
        """recv in thread"""
        return self._thread_broker.recv()
