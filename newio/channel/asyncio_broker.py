import logging
import asyncio
from queue import Empty, Full
from threading import Lock as ThreadLock
from socket import socketpair

from newio.api import spawn, run_in_asyncio
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


async def cond_notify_all(cond):
    await cond.acquire()
    try:
        cond.notify_all()
    finally:
        cond.release()


class AsyncioBroker:
    def __init__(self, queue):
        self._queue = queue
        self._notify_send, self._wakeup_send = socketpair()
        self._notify_recv, self._wakeup_recv = socketpair()
        self._notify_send.setblocking(False)
        self._wakeup_send.setblocking(False)
        self._notify_recv.setblocking(False)
        self._wakeup_recv.setblocking(False)
        self._send_cond = None
        self._recv_cond = None
        self._is_closed = False
        self._notify_lock = ThreadLock()
        self._consumer_broker_task = None
        self._producer_broker_task = None

    async def _init_cond(self):

        async def _init():
            self._send_cond = asyncio.Condition()
            self._recv_cond = asyncio.Condition()

        await run_in_asyncio(_init())

    async def _notify_all_cond(self):

        async def _notify_all():
            await cond_notify_all(self._send_cond)
            await cond_notify_all(self._recv_cond)

        await run_in_asyncio(_notify_all())

    async def start(self):
        LOG.debug('[starting] channel asyncio broker')
        await self._init_cond()
        self._consumer_broker_task = await spawn(self._consumer_broker())
        self._producer_broker_task = await spawn(self._producer_broker())
        LOG.debug('[started] channel asyncio broker')

    async def shutdown(self, wait=True):
        LOG.debug('[stopping] channel asyncio broker')
        self._is_closed = True
        with self._notify_lock:
            self._notify_send.sendall(b'x')
            self._notify_recv.sendall(b'x')
        self._notify_send.close()
        self._notify_recv.close()
        await self._notify_all_cond()
        if wait:
            await self._consumer_broker_task.join()
            await self._producer_broker_task.join()
            self._wakeup_send.close()
            self._wakeup_recv.close()
        LOG.debug('[stopped] channel asyncio broker')

    def notify_send(self):
        if self._is_closed:
            return
        with self._notify_lock:
            self._notify_send.sendall(b'1')

    def notify_recv(self):
        if self._is_closed:
            return
        with self._notify_lock:
            self._notify_recv.sendall(b'1')

    async def send(self, item):
        if self._is_closed:
            raise ChannelClosed()
        while True:
            try:
                self._queue.put_nowait(item)
            except Full:
                await cond_wait(self._send_cond)
                if self._is_closed:
                    raise ChannelClosed() from None
            else:
                break
        await cond_notify(self._recv_cond)

    async def recv(self):
        while True:
            try:
                item = self._queue.get_nowait()
            except Empty:
                if self._is_closed:
                    raise ChannelClosed() from None
                await cond_wait(self._recv_cond)
            else:
                break
        await cond_notify(self._send_cond)
        return item

    async def _consumer_broker(self):

        async def _broker():
            LOG.debug('[running] channel asyncio broker consumer')
            loop = asyncio.get_event_loop()
            while True:
                nbytes = await loop.sock_recv(self._wakeup_recv, 1)
                await cond_notify(self._recv_cond)
                if not nbytes or nbytes == b'x':
                    LOG.debug('[stopping] channel asyncio broker consumer')
                    break
            LOG.debug('[stopped] channel asyncio broker consumer')

        await run_in_asyncio(_broker())

    async def _producer_broker(self):

        async def _broker():
            LOG.debug('[running] channel asyncio broker producer')
            loop = asyncio.get_event_loop()
            while True:
                nbytes = await loop.sock_recv(self._wakeup_send, 1)
                await cond_notify(self._send_cond)
                if not nbytes or nbytes == b'x':
                    LOG.debug('[stopping] channel asyncio broker producer')
                    break
            LOG.debug('[stopped] channel asyncio broker producer')

        await run_in_asyncio(_broker())
