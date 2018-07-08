import logging
import asyncio

from newio.api import run_in_asyncio

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
    def __init__(self, controller):
        self.controller = controller
        self._cond_inited = False
        self._send_cond = None
        self._recv_cond = None
        self._sender_stopped = None
        self._receiver_stopped = None
        self._receiver_broker_started = False
        self._sender_broker_started = False

    def _enaure_cond_inited(self):
        if self._cond_inited:
            return
        self._cond_inited = True
        self._send_cond = asyncio.Condition()
        self._recv_cond = asyncio.Condition()
        self._sender_stopped = asyncio.Event()
        self._receiver_stopped = asyncio.Event()

    async def send(self, item):
        self._enaure_cond_inited()
        if not self._sender_broker_started:
            self._sender_broker_started = True
            await self._start_sender_broker()
        while True:
            ok = self.controller.try_send(item)
            if ok:
                break
            await cond_wait(self._send_cond)

    async def recv(self):
        self._enaure_cond_inited()
        if not self._receiver_broker_started:
            self._receiver_broker_started = True
            await self._start_receiver_broker()
        while True:
            ok, item = self.controller.try_recv()
            if ok:
                return item
            await cond_wait(self._recv_cond)

    async def join(self):
        await run_in_asyncio(self._asyncio_join())

    async def _stop_receiver_broker(self):
        await cond_notify_all(self._recv_cond)
        self._receiver_stopped.set()
        LOG.debug('[stopped] channel asyncio receiver broker')

    async def _start_receiver_broker(self):
        LOG.debug('[running] channel asyncio receiver broker')
        fd = self.controller.receiver_wait_fd
        loop = asyncio.get_event_loop()

        def callback():
            if self.controller.closed:
                LOG.debug('[stopping] channel asyncio receiver broker')
                loop.remove_reader(fd)
                loop.create_task(self._stop_receiver_broker())
            else:
                loop.create_task(cond_notify_all(self._recv_cond))

        loop.add_reader(fd, callback)

    async def _stop_sender_broker(self):
        await cond_notify_all(self._send_cond)
        self._sender_stopped.set()
        LOG.debug('[stopped] channel asyncio sender broker')

    async def _start_sender_broker(self):
        LOG.debug('[running] channel asyncio sender broker')
        fd = self.controller.sender_wait_fd
        loop = asyncio.get_event_loop()

        def callback():
            if self.controller.closed:
                LOG.debug('[stopping] channel asyncio sender broker')
                loop.remove_reader(fd)
                loop.create_task(self._stop_sender_broker())
            else:
                loop.create_task(cond_notify_all(self._send_cond))

        loop.add_reader(fd, callback)

    async def _asyncio_join(self):
        self._enaure_cond_inited()
        if self._receiver_broker_started:
            await self._receiver_stopped.wait()
        if self._sender_broker_started:
            await self._sender_stopped.wait()
