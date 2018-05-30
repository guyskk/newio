'''
Newio Kernel
'''
import os
import logging
import time
from collections import deque
from llist import dllist

from newio.syscall import TaskCanceled, TaskTimeout
from newio.syscall import Task as UserTask
from newio.syscall import Timeout as UserTimeout
from newio.syscall import Futex as UserFutex

from .timer import TimerWheel
from .executor import Executor
from .selector import Selector
from .futex import KernelFutex

LOG = logging.getLogger(__name__)
DEFAULT_TICK_DURATION = 0.010
DEFAULT_MAX_NUM_PROCESS = os.cpu_count()
DEFAULT_MAX_NUM_THREAD = DEFAULT_MAX_NUM_PROCESS * 16


class Runner:
    '''coroutine runner'''

    def __init__(self, *args, **kwargs):
        self.k_args = args
        self.k_kwargs = kwargs

    def __call__(self, *args, **kwargs):
        kernel = Kernel(*self.k_args, **self.k_kwargs)
        return kernel.run(*args, **kwargs)


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

    def clean_waiting(self):
        if self.waiting:
            self.waiting.clean()
            self.waiting = None

    def throw_cancel(self):
        for _ in range(100):
            self.throw(TaskCanceled())
        message = f'failed to cancel task {self!r}, it may leak resources'
        LOG.error(message)
        raise RuntimeError(message)


class Kernel:
    def __init__(
        self,
        max_num_thread=DEFAULT_MAX_NUM_THREAD,
        max_num_process=DEFAULT_MAX_NUM_PROCESS,
        tick_duration=DEFAULT_TICK_DURATION,
    ):
        self.next_task_id = 0
        self.tick_duration = tick_duration
        self.tasks = dllist()
        self.ready_tasks = deque()
        self.selector = Selector()
        self.timer_wheel = TimerWheel(tick_duration=tick_duration)
        self.executor = Executor(
            max_num_thread=max_num_thread, max_num_process=max_num_process)
        self.current = None
        self.main_task = None

    def run(self, coro, timeout=None):
        self.main_task = self.start_task(self.kernel_main(coro))
        if timeout is not None:
            self.timer_wheel.start_timer(
                timeout, self._timer_action_cancel, self.main_task)
        try:
            self._run()
        except BaseException:
            self.shutdown()
            raise
        else:
            self.close()
        if self.main_task.error:
            raise self.main_task.error
        return self.main_task.result

    async def kernel_main(self, coro):
        try:
            return await coro
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

    def close(self, wait=True):
        '''normal exit'''
        self.selector.close()
        self.executor.shutdown(wait=wait)

    def shutdown(self):
        '''force exit'''
        # force cancel all tasks
        self.tasks.remove(self.main_task.node)
        while self.tasks:
            task = self.tasks.first.value
            self._cancel_task(task)
        self.main_task.node = self.tasks.append(self.main_task)
        self._cancel_task(self.main_task)
        self.close(wait=False)

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
            self._poll_executor()  # executor
            self._run_ready_tasks()
            t2 = time.monotonic()
            time_poll = self.tick_duration / (1 + factor_timers + factor_tasks)
            self._poll_io(round(time_poll, 3))  # poll_io
            t3 = time.monotonic()
            self._run_ready_tasks()
            t4 = time.monotonic()
            poll_io_cost = max(0.001, t3 - t2)
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
        for task in self.selector.poll(time_poll):
            self.schedule_task(task, self._task_action_send)

    def _poll_executor(self):
        for task, result, error in self.executor.poll():
            if error is None:
                self.schedule_task(task, self._task_action_send, result)
            else:
                self.schedule_task(task, self._task_action_throw, error)
            task.clean_waiting()

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
        task.clean_waiting()
        for wakeup_task in task.stop_futex.wake_all():
            self.schedule_task(wakeup_task, self._task_action_send)
        # unregister
        self.tasks.remove(task.node)

    def _timer_action_wakeup(self, timer):
        task = timer.task
        LOG.debug('task %r wakeup by %r', task, timer)
        task.clean_waiting()
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
        self.current.clean_waiting()
        self.current.waiting = timer

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
        timer.stop()
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
        futex = KernelFutex.of(user_futex)
        waiter = futex.add_waiter(self.current)
        self.current.clean_waiting()
        self.current.waiting = waiter

    def nio_futex_wake(self, user_futex, n):
        futex = KernelFutex.of(user_futex)
        if n == UserFutex.WAKE_ALL:
            wakeup_tasks = futex.wake_all()
        else:
            wakeup_tasks = futex.wake(n)
        for task in wakeup_tasks:
            task.clean_waiting()
            self.schedule_task(task, self._task_action_send)
        self.schedule_task(self.current, self._task_action_send)

    def nio_wait_read(self, user_fd):
        fd = self.selector.register_read(self.current, user_fd)
        self.current.waiting = fd

    def nio_wait_write(self, user_fd):
        fd = self.selector.register_write(self.current, user_fd)
        self.current.waiting = fd

    def nio_run_in_thread(self, fn, args, kwargs):
        fut = self.executor.run_in_thread(self.current, fn, args, kwargs)
        self.current.clean_waiting()
        self.current.waiting = fut

    def nio_run_in_process(self, fn, args, kwargs):
        fut = self.executor.run_in_process(self.current, fn, args, kwargs)
        self.current.clean_waiting()
        self.current.waiting = fut
