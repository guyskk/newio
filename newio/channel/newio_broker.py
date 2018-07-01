import logging
from queue import Full, Empty
from threading import Lock as ThreadLock

from newio import spawn
from newio.sync import Condition
from newio.socket import socketpair
from .error import ChannelClosed

LOG = logging.getLogger(__name__)


class NewioBroker:
    def __init__(self, queue):
        self._queue = queue
        self._notify_send, self._wakeup_send = socketpair()
        self._notify_recv, self._wakeup_recv = socketpair()
        self._send_cond = Condition()
        self._recv_cond = Condition()
        self._is_closed = False
        self._notify_lock = ThreadLock()
        self._consumer_broker_task = None
        self._producer_broker_task = None

    async def start(self):
        LOG.debug('[starting] channel newio broker')
        self._consumer_broker_task = await spawn(self._consumer_broker())
        self._producer_broker_task = await spawn(self._producer_broker())
        LOG.debug('[started] channel newio broker')

    async def shutdown(self, wait=True):
        LOG.debug('[stopping] channel newio broker')
        self._is_closed = True
        with self._notify_lock:
            self._notify_send.socket.sendall(b'x')
            self._notify_recv.socket.sendall(b'x')
        await self._notify_send.close()
        await self._notify_recv.close()
        await self._send_cond.notify_all()
        await self._recv_cond.notify_all()
        if wait:
            await self._consumer_broker_task.join()
            await self._producer_broker_task.join()
            await self._wakeup_send.close()
            await self._wakeup_recv.close()
        LOG.debug('[stopped] channel newio broker')

    def notify_send(self):
        if self._is_closed:
            return
        with self._notify_lock:
            self._notify_send.socket.sendall(b'1')

    def notify_recv(self):
        if self._is_closed:
            return
        with self._notify_lock:
            self._notify_recv.socket.sendall(b'1')

    async def send(self, item):
        if self._is_closed:
            raise ChannelClosed()
        while True:
            try:
                self._queue.put_nowait(item)
            except Full:
                await self._send_cond.wait()
                if self._is_closed:
                    raise ChannelClosed() from None
            else:
                break
        await self._recv_cond.notify()

    async def recv(self):
        while True:
            try:
                item = self._queue.get_nowait()
            except Empty:
                if self._is_closed:
                    raise ChannelClosed() from None
                await self._recv_cond.wait()
            else:
                break
        await self._send_cond.notify()
        return item

    async def _consumer_broker(self):
        LOG.debug('[running] channel newio broker consumer')
        while True:
            nbytes = await self._wakeup_recv.recv(1)
            await self._recv_cond.notify()
            if not nbytes or nbytes == b'x':
                LOG.debug('[stopping] channel newio broker consumer')
                break
        LOG.debug('[stopped] channel newio broker consumer')

    async def _producer_broker(self):
        LOG.debug('[running] channel newio broker producer')
        while True:
            nbytes = await self._wakeup_send.recv(1)
            await self._send_cond.notify()
            if not nbytes or nbytes == b'x':
                LOG.debug('[stopping] channel newio broker producer')
                break
        LOG.debug('[stopped] channel newio broker producer')
