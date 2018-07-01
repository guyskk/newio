import logging
from queue import Empty, Full

from .error import ChannelClosed

LOG = logging.getLogger(__name__)


class ThreadBroker:
    def __init__(self, queue):
        self._queue = queue
        self._blocking_timeout = 0.1
        self._is_closed = False

    async def start(self):
        '''do nothing'''

    async def shutdown(self, wait=True):
        self._is_closed = True

    def notify_send(self):
        '''do nothing'''

    def notify_recv(self):
        '''do nothing'''

    def send(self, item):
        if self._is_closed:
            raise ChannelClosed()
        while True:
            try:
                self._queue.put(item, timeout=self._blocking_timeout)
            except Full:
                if self._is_closed:
                    raise ChannelClosed() from None
            else:
                break

    def recv(self):
        if self._is_closed:
            try:
                return self._queue.get_nowait()
            except Empty:
                raise ChannelClosed() from None
        while True:
            try:
                item = self._queue.get(timeout=self._blocking_timeout)
            except Empty:
                if self._is_closed:
                    raise ChannelClosed() from None
            else:
                break
        return item
