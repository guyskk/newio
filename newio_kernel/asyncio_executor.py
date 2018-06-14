import logging
import asyncio
from concurrent.futures import Future
from socket import socketpair
from threading import Thread

LOG = logging.getLogger(__name__)


class AsyncioExecutor:
    def __init__(self):
        self._request = Future()
        self._response = Future()
        self._notify, self._wakeup = socketpair()
        self._notify.setblocking(False)
        self._wakeup.setblocking(False)
        self._worker = Thread(target=self.asyncio_worker)
        self._worker.start()

    def submit(self, coro):
        self._request.set_result(coro)
        self._notify.sendall(b'1')
        fut = self._response.result()
        self._response = Future()
        return fut

    def shutdown(self, wait=True):
        self._notify.sendall(b'x')
        self._notify.close()
        if wait:
            self._worker.join()

    def asyncio_worker(self):
        LOG.debug('asyncio executor worker starting')
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._asyncio_worker(loop))
        loop.close()

    async def _asyncio_worker(self, loop):
        while True:
            nbytes = await loop.sock_recv(self._wakeup, 1)
            if not nbytes or nbytes == b'x':
                LOG.debug('asyncio executor worker exiting')
                self._wakeup.close()
                break
            coro = self._request.result()
            self._request = Future()
            self._response.set_result(loop.create_task(coro))
