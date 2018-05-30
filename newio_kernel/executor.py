from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from concurrent.futures import CancelledError as FutureCancelledError
from queue import Queue as ThreadQueue
from queue import Empty as QUEUE_EMPTY


class ExecutorFuture:
    def __init__(self, message_queue, task, future):
        self._message_queue = message_queue
        self._task = task
        self._fut = future
        self._cancelled = False
        future.add_done_callback(self._on_fn_done)

    def cancel(self):
        self._cancelled = True
        self._fut.cancel()

    def _on_fn_done(self, fut):
        if self._cancelled:
            return
        result = error = None
        try:
            error = fut.exception()
            if error is None:
                result = fut.result()
        except FutureCancelledError:
            pass  # ignore
        else:
            self._message_queue.put((self._task, result, error))


class Executor:
    def __init__(self, max_threads=None, max_processes=None):
        self._thread_executor = ThreadPoolExecutor(max_threads)
        self._process_executor = ProcessPoolExecutor(max_processes)
        self._message_queue = ThreadQueue()

    def run_in_thread(self, task, fn, args, kwargs):
        fut = self._thread_executor.submit(fn, *args, **kwargs)
        return ExecutorFuture(self._message_queue, task, fut)

    def run_in_process(self, task, fn, args, kwargs):
        fut = self._process_executor.submit(fn, *args, **kwargs)
        return ExecutorFuture(self._message_queue, task, fut)

    def poll(self):
        while True:
            try:
                task, result, error = self._message_queue.get_nowait()
            except QUEUE_EMPTY:
                break
            else:
                yield (task, result, error)

    def shutdown(self, wait=True):
        self._thread_executor.shutdown(wait=wait)
        self._process_executor.shutdown(wait=wait)
