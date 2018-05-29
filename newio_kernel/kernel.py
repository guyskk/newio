import logging
import time
import selectors
from selectors import DefaultSelector
from collections import deque
from llist import dllist
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from concurrent.futures import CancelledError as FutureCancelledError
from queue import Queue as ThreadQueue
from queue import Empty as QUEUE_EMPTY

from newio.syscall import TaskCanceled, TaskTimeout
from newio.syscall import Task as UserTask
from newio.syscall import Timeout as UserTimeout
from newio.syscall import Futex as UserFutex

from .timer import TimerWheel

LOG = logging.getLogger(__name__)
DEFAULT_TICK_DURATION = 0.010


class Runner:

    def __init__(self, *args, **kwargs):
        self.k_args = args
        self.k_kwargs = kwargs

    def __call__(self, *args, **kwargs):
        kernel = Kernel(*self.k_args, **self.k_kwargs)
        return kernel.run(*args, **kwargs)


class KernelFd:

    def __init__(self, user_fd, task):
        self._nio_ref_ = user_fd
        user_fd._nio_ref_ = self
        self.task = task
        self.selector_key = None

    @property
    def events(self):
        if self.selector_key is None:
            return 0
        return self.selector_key.events

    def fileno(self):
        return self._nio_ref_.fileno()

    def __repr__(self):
        return '<KernelFd#{} of task {}>'.format(
            self.fileno(), self.task)


class WaitingIO:

    def __init__(self, fd):
        self.fd = fd

    def state(self):
        if self.fd.events == selectors.EVENT_READ:
            return 'wait_read'
        elif self.fd.events == selectors.EVENT_WRITE:
            return 'wait_write'
        else:
            return 'wait_io'

    def clean(self, kernel):
        if self.fd is not None and self.fd.selector_key is not None:
            kernel.selector.unregister(self.fd)


class WaitingExecutor:

    def __init__(self, future):
        self.future = future

    def state(self):
        return 'wait_executor'

    def clean(self, kernel):
        LOG.debug('cancel fn of %r', self)
        if self.future.done():
            return
        self.future.cancel()


class WaitingFutex:

    def __init__(self, futex, node):
        self.futex = futex
        self.node = node

    def state(self):
        return 'wait_futex'

    def clean(self, kernel):
        if self.node in self.futex.waiters:
            self.futex.waiters.remove(self.node)


class WaitingTimer:
    def __init__(self, timer):
        self.timer = timer

    def state(self):
        return 'sleep'

    def clean(self, kernel):
        kernel.timer_wheel.stop_timer(self.timer)


class KernelTask:
    def __init__(self, coro, *, ident):
        self._nio_ref_ = None
        self.coro = coro
        self.send = coro.send
        self.throw = coro.throw
        self.ident = ident
        self.name = getattr(coro, '__name__', str(coro))
        self.is_alive = True
        self.node = None
        self.stop_futex = KernelFutex(UserFutex())
        self.error = None
        self.result = None
        self.waiting = None

    def __repr__(self):
        if self.is_alive:
            if self.waiting is None:
                state = 'ready'
            else:
                state = self.waiting.state()
        elif self.error:
            state = 'error'
        else:
            state = 'stoped'
        return '<KernelTask#{} {} @{}>'.format(self.ident, self.name, state)

    def throw_cancel(self):
        for _ in range(100):
            self.throw(TaskCanceled())
        message = f'failed to cancel task {self!r}, it may leak resources'
        LOG.error(message)
        raise RuntimeError(message)


class KernelFutex:

    def __init__(self, user_futex):
        self._nio_ref_ = user_futex
        user_futex._nio_ref_ = self
        self.waiters = dllist()

    def __repr__(self):
        num = len(self.waiters)
        return f'<KernelFutex@{hex(id(self._nio_ref_))} {num} waiters>'


class Kernel:
    def __init__(self, tick_duration=DEFAULT_TICK_DURATION):
        self.next_task_id = 0
        self.tick_duration = tick_duration
        self.tasks = dllist()
        self.ready_tasks = deque()
        self.selector = DefaultSelector()
        self.timer_wheel = TimerWheel(tick_duration=tick_duration)
        self.thread_executor = ThreadPoolExecutor()
        self.process_executor = ProcessPoolExecutor()
        self.executor_messages = ThreadQueue()
        self.current = None
        self.main_task = None

    def run(self, coro, timeout=None):
        self.main_task = self.start_task(self.kernel_main(coro, timeout))
        if timeout is not None:
            self.timer_wheel.start_timer(
                timeout, self._timer_action_cancel, self.main_task)
        try:
            self._run()
        except:
            self.shutdown()
            self._close(wait=False)
            raise
        else:
            self._close()
        if self.main_task.error:
            raise self.main_task.error
        return self.main_task.result

    def _close(self, wait=True):
        self.thread_executor.shutdown(wait=wait)
        self.process_executor.shutdown(wait=wait)

    async def kernel_main(self, coro, timeout):
        try:
            await coro
        finally:
            # when main task exiting, normally cancel all subtasks
            for task in self.tasks:
                if task is not self.main_task:
                    user_task = task._nio_ref_
                    await user_task.cancel()
            for task in self.tasks:
                if task is not self.main_task:
                    user_task = task._nio_ref_
                    await user_task.join()

    def shutdown(self):
        # force cancel all tasks
        self.tasks.remove(self.main_task.node)
        while self.tasks:
            task = self.tasks.first.value
            self._cancel_task(task)
        self.main_task.node = self.tasks.append(self.main_task)
        self._cancel_task(self.main_task)

    def _cancel_task(self, task):
        # cancel a task, ignore any exception here because shutdown
        # caused by some exception, we need force cancel task
        try:
            task.throw_cancel()
        except StopIteration as stoped:
            self.stop_task(task, result=stoped.value)
        except TaskCanceled as ex:
            self.stop_task(task, error=ex)
        except BaseException as ex:
            LOG.warn('task %r crashed on cancel:', task, exc_info=True)
            self.stop_task(task, error=ex)

    def _run(self):
        factor_timers = 0  # timers_cost / poll_io_cost
        factor_tasks = 0  # tasks_cost / poll_io_cost
        while self.tasks:
            t1 = time.monotonic()
            self.timer_wheel.check()  # timer
            self._run_ready_tasks()
            self._check_executor_messages()  # executor
            self._run_ready_tasks()
            t2 = time.monotonic()
            time_poll = self.tick_duration / (1 + factor_timers + factor_tasks)
            self._poll_io(time_poll)  # poll_io
            t3 = time.monotonic()
            self._run_ready_tasks()
            t4 = time.monotonic()
            poll_io_cost = max(0.000001, t3 - t2)
            factor_timers = (t2 - t1) / poll_io_cost
            factor_tasks = (t4 - t3) / poll_io_cost

    def _run_ready_tasks(self):
        while self.ready_tasks:
            self.current, action, *value = self.ready_tasks.popleft()
            if not self.current.is_alive:
                LOG.debug('task %r not alive', self.current)
                continue
            try:
                call, *args = action(*value)
            except StopIteration as stoped:
                self.stop_task(self.current, result=stoped.value)
                continue
            except TaskCanceled as ex:
                self.stop_task(self.current, error=ex)
                continue
            except Exception as ex:
                LOG.warn('task %r crash:', self.current, exc_info=True)
                self.stop_task(self.current, error=ex)
                continue
            except BaseException as ex:
                self.stop_task(self.current, error=ex)
                raise
            handler = getattr(self, call.__name__, None)
            if handler is None:
                raise RuntimeError('unknown syscall {}'.format(call.__name__))
            handler(*args)

    def _poll_io(self, time_poll):
        io_events = self.selector.select(timeout=time_poll)
        for key, mask in io_events:
            fd = key.fileobj
            self.schedule_task(fd.task, self._task_action_send)

    def start_task(self, coro):
        # create
        task = KernelTask(coro, ident=self.next_task_id)
        LOG.debug('start task %r', task)
        self.next_task_id = (self.next_task_id + 1) % 2**32
        # register
        node = self.tasks.append(task)
        task.node = node
        # schedule
        self.schedule_task(task, self._task_action_send)
        return task

    def schedule_task(self, task, action, *value):
        LOG.debug('schedule task %r', task)
        self.ready_tasks.append((task, action, *value))

    def stop_task(self, task, *, result=None, error=None):
        if error:
            LOG.debug('stop task %s with error:', task, exc_info=error)
        else:
            LOG.debug('stop task %s', task)
        task.is_alive = False
        task.result = result
        task.error = error
        # cleanup
        if task.waiting:
            task.waiting.clean(self)
            task.waiting = None
        for wakeup_task in task.stop_futex.waiters:
            if wakeup_task.is_alive:
                self.schedule_task(wakeup_task, self._task_action_send)
        task.stop_futex.waiters.clear()
        # unregister
        self.tasks.remove(task.node)

    def _timer_action_wakeup(self, timer):
        task = timer.task
        LOG.debug('task %r wakeup by %r', task, timer)
        task.waiting = None
        self.schedule_task(task, self._task_action_send)

    def _timer_action_timeout(self, timer):
        task = timer.task
        LOG.debug('task %r timeout by %r', task, timer)
        self.schedule_task(task, self._task_action_timeout)

    def _timer_action_cancel(self, timer):
        task = timer.task
        LOG.debug('task %r cancel by %r', task, timer)
        self.schedule_task(task, self._task_action_cancel)

    def _task_action_timeout(self):
        LOG.debug('throw timeout to task %r', self.current)
        return self.current.throw(TaskTimeout())

    def _task_action_cancel(self):
        LOG.debug('throw cancel to task %r', self.current)
        self.current.throw_cancel()

    def _task_action_throw(self, error):
        LOG.debug('throw %r to task %r', error, self.current)
        return self.current.throw(error)

    def _task_action_send(self, value=None):
        LOG.debug('send %r to task %r', value, self.current)
        return self.current.send(value)

    def nio_sleep(self, seconds):
        timer = self.timer_wheel.start_timer(
            seconds, self._timer_action_wakeup, self.current)
        if self.current.waiting:
            self.current.waiting.clean(self)
        self.current.waiting = WaitingTimer(timer)

    def nio_set_timeout(self, seconds):
        timer = self.timer_wheel.start_timer(
            seconds, self._timer_action_timeout, self.current)
        user_timeout = UserTimeout(timer)
        self.schedule_task(self.current, self._task_action_send, user_timeout)

    def nio_unset_timeout(self, user_timeout):
        timer = user_timeout._nio_ref_
        if timer is None:
            raise RuntimeError(
                'timeout {!r} not set in kernel'.format(user_timeout))
        self.timer_wheel.stop_timer(timer)
        self.schedule_task(self.current, self._task_action_send)

    def nio_spawn(self, coro):
        task = self.start_task(coro)
        user_task = UserTask(task)
        self.schedule_task(self.current, self._task_action_send, user_task)

    def nio_cancel(self, user_task):
        task = user_task._nio_ref_
        self.schedule_task(task, self._task_action_cancel)
        # make sure syscall nio_cancel not raise TaskCanceled
        self.ready_tasks.appendleft((self.current, self._task_action_send))

    def nio_join(self, user_task):
        task = user_task._nio_ref_
        if task.is_alive:
            self.nio_futex_wait(task.stop_futex._nio_ref_)
        else:
            self.schedule_task(self.current, self._task_action_send)

    def nio_current_task(self):
        user_task = self.current._nio_ref_
        self.schedule_task(self.current, self._task_action_send, user_task)

    def nio_futex_wait(self, user_futex):
        futex = user_futex._nio_ref_
        if futex is None:
            futex = KernelFutex(user_futex)
        node = futex.waiters.append(self.current)
        LOG.debug('task %r waiting for futex %r', self.current, futex)
        if self.current.waiting:
            self.current.waiting.clean(self)
        self.current.waiting = WaitingFutex(futex, node)

    def nio_futex_wake(self, user_futex, n):
        futex = user_futex._nio_ref_
        if futex is None:
            futex = KernelFutex(user_futex)
        if n == UserFutex.WAKE_ALL:
            for task in futex.waiters:
                if task.is_alive:
                    self.schedule_task(task, self._task_action_send)
            futex.waiters.clear()
        else:
            while futex.waiters and n > 0:
                task = futex.popleft()
                if task.is_alive:
                    self.schedule_task(task, self._task_action_send)
                    n -= 1
        self.schedule_task(self.current, self._task_action_send)

    # def _selector_unregister(self, task):
    #     fd = task.waiting_fd
    #     LOG.debug('selector unregister fd %r', fd)
    #     if fd is not None and fd.selector_key is not None:
    #         self.selector.unregister(fd)
    #     task.waiting_fd = None

    def _selector_register(self, task, user_fd, events):
        fd = user_fd._nio_ref_
        if fd is None:
            fd = KernelFd(user_fd, task)
        if fd.task is not task:
            raise RuntimeError('file descriptor already waiting by other task')
        LOG.debug('selector register fd %r, events=%r', fd, events)
        if fd.selector_key is None:
            fd.selector_key = self.selector.register(fd, events)
        elif fd.events != events:
            fd.selector_key = self.selector.modify(fd, events)
        task.waiting = WaitingIO(fd)

    def nio_wait_read(self, user_fd):
        self._selector_register(self.current, user_fd, selectors.EVENT_READ)

    def nio_wait_write(self, user_fd):
        self._selector_register(self.current, user_fd, selectors.EVENT_WRITE)

    def nio_run_in_thread(self, fn, args, kwargs):
        self._run_in_executor(self.thread_executor, fn, args, kwargs)

    def nio_run_in_process(self, fn, args, kwargs):
        self._run_in_executor(self.process_executor, fn, args, kwargs)

    def _run_in_executor(self, executor, fn, args, kwargs):
        fut = executor.submit(fn, *args, **kwargs)
        fut.add_done_callback(self._on_fn_done(self.current))
        self.current.waiting = WaitingExecutor(fut)

    def _on_fn_done(self, task):
        def callback(fut):
            LOG.debug('fn is done, future=%r, task=%r', fut, task)
            try:
                msg = (task, fut.result(), fut.exception())
            except FutureCancelledError:
                pass  # task already stoped, ignore the exception
            else:
                self.executor_messages.put(msg)
        return callback

    def _check_executor_messages(self):
        while True:
            try:
                task, result, error = self.executor_messages.get_nowait()
            except QUEUE_EMPTY:
                break
            else:
                self._notify_fn_task(task, result, error)

    def _notify_fn_task(self, task, result, error):
        if error is None:
            LOG.debug('notify fn task %r', task)
        else:
            LOG.debug('notify fn task %r with error:', task, exc_info=error)
        if not task.is_alive:
            return
        if error is None:
            self.schedule_task(task, self._task_action_send, result)
        else:
            self.schedule_task(task, self._task_action_throw, error)
        task.waiting = None
