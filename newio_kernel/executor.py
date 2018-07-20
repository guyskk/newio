import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from concurrent.futures import CancelledError as FutureCancelledError
from threading import Lock as ThreadLock
from queue import Queue as ThreadQueue
from queue import Empty

from newio import spawn
from newio.socket import socketpair

from .asyncio_executor import AsyncioExecutor

LOG = logging.getLogger(__name__)


class ExecutorFuture:
    def __init__(self, handler, task, future):
        self._handler = handler
        self.task = task
        self.error = None
        self.result = None
        self._fut = future
        self._cancelled = False
        future.add_done_callback(self._on_fn_done)

    def cancel(self):
        if not self._cancelled:
            LOG.debug('task %r cancel executor', self.task)
            self._cancelled = True
            self._fut.cancel()

    def cancelled(self):
        return self._cancelled

    def state(self):
        return 'wait_executor'

    def clean(self):
        self.cancel()

    def _on_fn_done(self, fut):
        if self._cancelled:
            return
        result = error = None
        try:
            error = fut.exception()
            if error is None:
                result = fut.result()
        except FutureCancelledError:
            return  # ignore
        if error:
            LOG.debug('executor for task %r crashed:', self.task, exc_info=error)
        else:
            LOG.debug('executor for task %r finished', self.task)
        self.error = error
        self.result = result
        self._handler(self)


class Executor:
    def __init__(self, handler, max_num_thread=None, max_num_process=None):
        self._handler = handler
        self._thread_executor = ThreadPoolExecutor(max_num_thread)
        self._process_executor = ProcessPoolExecutor(max_num_process)
        self._asyncio_executor = AsyncioExecutor()
        self._notify, self._wakeup = socketpair()
        self._queue = ThreadQueue(1024)
        self.agent_task = None
        self._is_exiting = False
        self._notify_lock = ThreadLock()

    def run_in_thread(self, task, fn, args, kwargs):
        LOG.debug('task %r run %r in thread executor', task, fn)
        fut = self._thread_executor.submit(fn, *args, **kwargs)
        return ExecutorFuture(self.handler, task, fut)

    def run_in_process(self, task, fn, args, kwargs):
        LOG.debug('task %r run %r in process executor', task, fn)
        fut = self._process_executor.submit(fn, *args, **kwargs)
        return ExecutorFuture(self.handler, task, fut)

    def run_in_asyncio(self, task, coro):
        LOG.debug('task %r run %r in asyncio executor', task, coro)
        fut = self._asyncio_executor.submit(coro)
        return ExecutorFuture(self.handler, task, fut)

    async def executor_agent(self):
        LOG.debug('[running] executor agent')
        is_exiting = False
        while True:
            try:
                fut = self._queue.get_nowait()
            except Empty:
                if is_exiting:
                    break
                nbytes = await self._wakeup.recv(128)
                if not nbytes or b'x' in nbytes:
                    LOG.debug('[stopping] executor agent')
                    is_exiting = True
                    await self._wakeup.close()
            else:
                if not fut.cancelled():
                    self._handler(fut.task, fut.result, fut.error)
        LOG.debug('[stopped] executor agent')

    async def start(self):
        LOG.debug('[starting] executor')
        self.agent_task = await spawn(self.executor_agent())
        LOG.debug('[started] executor')

    async def stop(self):
        LOG.debug('[stopping] executor')
        self._is_exiting = True
        with self._notify_lock:
            await self._notify.sendall(b'x')
            await self._notify.close()
        await self.agent_task.join()
        self.shutdown()
        LOG.debug('[stopped] executor')

    def shutdown(self, wait=True):
        self._thread_executor.shutdown(wait=wait)
        self._process_executor.shutdown(wait=wait)
        self._asyncio_executor.shutdown(wait=wait)

    def handler(self, fut):
        self._queue.put_nowait(fut)
        if self._is_exiting:
            return
        with self._notify_lock:
            with self._notify.blocking() as notify:
                notify.sendall(b'1')
