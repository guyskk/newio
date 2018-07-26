import logging

from newio import spawn
from newio.sync import Condition
from newio.api import wait_read


LOG = logging.getLogger(__name__)


class NewioBroker:
    def __init__(self, controller):
        self.controller = controller
        self._send_cond = Condition()
        self._recv_cond = Condition()
        self._receiver_broker_task = None
        self._sender_broker_task = None

    async def send(self, item):
        if self._sender_broker_task is None:
            self._sender_broker_task = await spawn(self._sender_broker())
        while True:
            ok = self.controller.try_send(item)
            if ok:
                break
            await self._send_cond.wait()

    async def recv(self):
        if self._receiver_broker_task is None:
            self._receiver_broker_task = await spawn(self._receiver_broker())
        while True:
            ok, item = self.controller.try_recv()
            if ok:
                return item
            await self._recv_cond.wait()

    async def join(self):
        if self._sender_broker_task is not None:
            await self._sender_broker_task.join()
        if self._receiver_broker_task is not None:
            await self._receiver_broker_task.join()

    async def _receiver_broker(self):
        LOG.debug('[running] channel newio receiver broker')
        fd = self.controller.receiver_wait_fd
        while True:
            await wait_read(fd)
            if self.controller.closed:
                LOG.debug('[stopping] channel newio receiver broker')
                await self._recv_cond.notify_all()
                break
            else:
                await self._recv_cond.notify_all()
        LOG.debug('[stopped] channel newio receiver broker')

    async def _sender_broker(self):
        LOG.debug('[running] channel newio sender broker')
        fd = self.controller.sender_wait_fd
        while True:
            await wait_read(fd)
            if self.controller.closed:
                LOG.debug('[stopping] channel newio sender broker')
                await self._send_cond.notify_all()
                break
            else:
                await self._send_cond.notify_all()
        LOG.debug('[stopped] channel newio sender broker')
