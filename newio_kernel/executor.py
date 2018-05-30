import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from concurrent.futures import CancelledError as FutureCancelledError
from queue import Queue as ThreadQueue
from queue import Empty as QUEUE_EMPTY

LOG = logging.getLogger(__name__)


class ExecutorFuture:
    def __init__(self, message_queue, task, future):
        self._message_queue = message_queue
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
        if error:
            LOG.debug('executor for task %r finished with error',
                      self._task, exc_info=error)
        else:
            LOG.debug('executor for task %r finished', self._task)
        self._is_expired = True
        self._message_queue.put((self._task, result, error))


class Executor:
    def __init__(self, max_num_thread=None, max_num_process=None):
        self._thread_executor = ThreadPoolExecutor(max_num_thread)
        self._process_executor = ProcessPoolExecutor(max_num_process)
        self._message_queue = ThreadQueue()

    def run_in_thread(self, task, fn, args, kwargs):
        LOG.debug('task %r run %r in executor thread', task, fn)
        fut = self._thread_executor.submit(fn, *args, **kwargs)
        return ExecutorFuture(self._message_queue, task, fut)

    def run_in_process(self, task, fn, args, kwargs):
        LOG.debug('task %r run %r in executor process', task, fn)
        fut = self._process_executor.submit(fn, *args, **kwargs)
        return ExecutorFuture(self._message_queue, task, fut)

    def poll(self):
        while True:
            try:
                task, result, error = self._message_queue.get_nowait()
            except QUEUE_EMPTY:
                break
            else:
                if task.is_alive:
                    LOG.debug('task %r wakeup by executor', task)
                    yield (task, result, error)

    def shutdown(self, wait=True):
        self._thread_executor.shutdown(wait=wait)
        self._process_executor.shutdown(wait=wait)
