import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from concurrent.futures import CancelledError as FutureCancelledError

import newio
from .channel import Channel

LOG = logging.getLogger(__name__)


class ExecutorFuture:
    def __init__(self, executor, task, future):
        self._on_error = executor.on_error
        self._on_result = executor.on_result
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
            LOG.debug('executor for task %r finished with error',
                      self._task, exc_info=error)
            self._on_error(self._task, error)
        else:
            LOG.debug('executor for task %r finished', self._task)
            self._on_result(self._task, result)


class Executor:
    def __init__(self, on_error, on_result, max_num_thread=None, max_num_process=None):
        self._on_error_handler = on_error
        self._on_result_handler = on_result
        self._thread_executor = ThreadPoolExecutor(max_num_thread)
        self._process_executor = ProcessPoolExecutor(max_num_process)
        self._channel = Channel()

    def run_in_thread(self, task, fn, args, kwargs):
        LOG.debug('task %r run %r in executor thread', task, fn)
        fut = self._thread_executor.submit(fn, *args, **kwargs)
        return ExecutorFuture(self, task, fut)

    def run_in_process(self, task, fn, args, kwargs):
        LOG.debug('task %r run %r in executor process', task, fn)
        fut = self._process_executor.submit(fn, *args, **kwargs)
        return ExecutorFuture(self, task, fut)

    async def start(self):
        await newio.spawn(self._channel.consumer())

    def shutdown(self, wait=True):
        self._thread_executor.shutdown(wait=wait)
        self._process_executor.shutdown(wait=wait)

    def on_error(self, task, error):
        self._channel.send(self._on_error_handler, task, error)

    def on_result(self, task, result):
        self._channel.send(self._on_result_handler, task, result)
