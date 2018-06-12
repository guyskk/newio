import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from concurrent.futures import CancelledError as FutureCancelledError

from newio import spawn
from newio.channel import Channel

LOG = logging.getLogger(__name__)


class ExecutorFuture:
    def __init__(self, handler, task, future):
        self._handler = handler
        self._task = task
        self._fut = future
        self._is_expired = False
        future.add_done_callback(self._on_fn_done)

    def cancel(self):
        if not self._is_expired:
            LOG.debug('task %r cancel executor', self._task)
            self._fut.cancel()
            self._is_expired = True

    def state(self):
        return 'wait_executor'

    def clean(self):
        self.cancel()

    def _on_fn_done(self, fut):
        if self._is_expired:
            return
        result = error = None
        try:
            error = fut.exception()
            if error is None:
                result = fut.result()
        except FutureCancelledError:
            return  # ignore
        self._is_expired = True
        if error:
            LOG.debug('executor for task %r crashed:', self._task, exc_info=error)
        else:
            LOG.debug('executor for task %r finished', self._task)
        self._handler(self._task, result, error)


class Executor:
    def __init__(self, handler, max_num_thread=None, max_num_process=None):
        self._handler = handler
        self._thread_executor = ThreadPoolExecutor(max_num_thread)
        self._process_executor = ProcessPoolExecutor(max_num_process)
        self._channel = Channel()

    def run_in_thread(self, task, fn, args, kwargs):
        LOG.debug('task %r run %r in executor thread', task, fn)
        fut = self._thread_executor.submit(fn, *args, **kwargs)
        return ExecutorFuture(self.handler, task, fut)

    def run_in_process(self, task, fn, args, kwargs):
        LOG.debug('task %r run %r in executor process', task, fn)
        fut = self._process_executor.submit(fn, *args, **kwargs)
        return ExecutorFuture(self.handler, task, fut)

    async def executor_main(self):
        async for task, result, error in self._channel:
            LOG.debug('recv task %r from channel', task)
            self._handler(task, result, error)

    async def start(self):
        await self._channel.__aenter__()
        await spawn(self.executor_main())

    async def stop(self):
        await self._channel.__aexit__()

    def shutdown(self, wait=True):
        self._thread_executor.shutdown(wait=wait)
        self._process_executor.shutdown(wait=wait)

    def handler(self, task, result, error):
        LOG.debug('send task %r to channel', task)
        self._channel.send((task, result, error))
