import logging
import asyncio
from concurrent.futures import Future, CancelledError
from socket import socketpair
from threading import Thread
from queue import Queue as ThreadQueue
from queue import Empty

LOG = logging.getLogger(__name__)


class AsyncioExecutorFuture:
    def __init__(self, response):
        self._fut = Future()
        self._response = response
        response.add_done_callback(self._on_response_done)
        self._asyncio_task = None
        self._canceled = False

    def __getattr__(self, name):
        return getattr(self._fut, name)

    def cancel(self):
        if self._canceled:
            return
        self._canceled = True
        self._fut.cancel()
        return True

    def _on_task_done(self, asyncio_task):
        if self._canceled:
            return
        error = result = None
        try:
            error = asyncio_task.exception()
            if error is None:
                result = asyncio_task.result()
        except CancelledError:
            self._fut.cancel()
        else:
            if error is None:
                self._fut.set_result(result)
            else:
                self._fut.set_exception(error)

    def _on_response_done(self, response):
        if self._canceled:
            return
        try:
            asyncio_task = response.result()
        except CancelledError:
            self._fut.cancel()
        else:
            self._asyncio_task = asyncio_task
            asyncio_task.add_done_callback(self._on_task_done)


class AsyncioExecutor:
    def __init__(self):
        self._request = ThreadQueue(1024)
        self._notify, self._wakeup = socketpair()
        self._notify.setblocking(False)
        self._wakeup.setblocking(False)
        self._worker = Thread(target=self.asyncio_worker)
        self._worker.start()

    def submit(self, coro):
        response = Future()
        fut = AsyncioExecutorFuture(response)
        self._request.put_nowait((coro, response))
        self._notify.sendall(b'1')
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
        loop.create_task(self._asyncio_worker())
        loop.run_forever()
        loop.close()

    async def _asyncio_worker(self):
        loop = asyncio.get_event_loop()
        is_exiting = False
        while True:
            try:
                coro, response = self._request.get_nowait()
            except Empty:
                if is_exiting:
                    self._wakeup.close()
                    loop.stop()
                    break
                nbytes = await loop.sock_recv(self._wakeup, 128)
                if not nbytes or b'x' in nbytes:
                    LOG.debug('asyncio executor worker exiting')
                    is_exiting = True
            else:
                LOG.debug('asyncio executor worker execute %r', coro)
                response.set_result(loop.create_task(coro))
