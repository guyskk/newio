import logging

from .error import ChannelClosed
from .controller import ChannelController
from .newio_broker import NewioBroker
from .thread_broker import ThreadBroker


LOG = logging.getLogger(__name__)


class Channel:
    """Message channel for communicating between threads/newio"""

    def __init__(self, bufsize=None):
        self.controller = ChannelController(bufsize)
        self._thread_broker = ThreadBroker(self.controller)
        self._newio_broker = NewioBroker(self.controller)
        self.sync = SyncChannel(self._thread_broker)

    async def close(self):
        self.controller.close()

    async def __aenter__(self):
        if self.controller.closed:
            raise RuntimeError('Channel already closed')
        return self

    async def __aexit__(self, *exc_info):
        await self.close()
        await self._thread_broker.join()
        await self._newio_broker.join()
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


class SyncChannel:
    def __init__(self, broker):
        self._broker = broker

    def __iter__(self):
        while True:
            try:
                yield self.recv()
            except ChannelClosed:
                break

    def send(self, item):
        """send in thread"""
        self._broker.send(item)

    def recv(self):
        """recv in thread"""
        return self._broker.recv()
