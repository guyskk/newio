import logging
import time
import newio.api as nio
from selectors import DefaultSelector
from collections import deque
from llist import dllist

from .timer import TimerWheel

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)


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
        self.stop_event = nio.Event()
        self.error = None
        self.result = None

    def __repr__(self):
        if self.is_alive:
            state = 'alive'
        elif self.error:
            state = 'error'
        else:
            state = 'result'
        return '<KernelTask#{} {} @{}>'.format(self.ident, self.name, state)


class KernelEvent:

    def __init__(self, user_event):
        self._nio_ref_ = user_event
        user_event._nio_ref_ = self
        self.waiting_tasks = []
        self.is_set = False

    def __repr__(self):
        return '<KernelEvent@{} is_set={}>'.format(hex(id(self)), self.is_set)

    def add_waiting_task(self, task):
        LOG.debug('task %r waiting for event %r', task, self)
        self.waiting_tasks.append(task)


DEFAULT_TICK_DURATION = 0.010


class Kernel:
    def __init__(self, coro, tick_duration=DEFAULT_TICK_DURATION):
        self.next_task_id = 0
        self.tick_duration = tick_duration
        self.tasks = dllist()
        self.ready_tasks = deque()
        self.current = None
        self.selector = DefaultSelector()
        self.timer_wheel = TimerWheel(tick_duration=tick_duration)
        self.start_task(coro)

    def run(self):
        factor_timers = 0  # timers_cost / poll_io_cost
        factor_tasks = 0  # tasks_cost / poll_io_cost
        while self.tasks:
            t1 = time.monotonic()
            self.timer_wheel.check()
            self._run_ready_tasks()
            t2 = time.monotonic()
            time_poll = self.tick_duration / (1 + factor_timers + factor_tasks)
            self._poll_io(time_poll)
            t3 = time.monotonic()
            self._run_ready_tasks()
            t4 = time.monotonic()
            poll_io_cost = max(0.000001, t3 - t2)
            factor_timers = (t2 - t1) / poll_io_cost
            factor_tasks = (t4 - t3) / poll_io_cost

    def schedule(self, task, action, *value):
        LOG.debug('schedule task %r', task)
        self.ready_tasks.append((task, action, *value))

    def notify(self, user_event):
        event = user_event._nio_ref_
        if event is None:
            event = KernelEvent(user_event)
        if event.is_set:
            LOG.debug('event %r already notified', event)
            return
        event.is_set = True
        LOG.debug('notify event %r', event)
        for task in event.waiting_tasks:
            self.schedule(task, self._task_action_send)

    def start_task(self, coro):
        task = KernelTask(coro, ident=self.next_task_id)
        LOG.debug('start task %s', task)
        self.next_task_id = (self.next_task_id + 1) % 2**32
        node = self.tasks.append(task)
        task.node = node
        self.schedule(task, self._task_action_send)
        return task

    def stop_task(self, task, *, result=None, error=None):
        if error:
            LOG.debug('stop task %s with error:', task, exc_info=error)
        else:
            LOG.debug('stop task %s', task)
        task.is_alive = False
        task.result = result
        task.error = error
        self.notify(task.stop_event)
        self.tasks.remove(task.node)

    def _timer_action_wakeup(self, timer):
        task = timer.task
        LOG.debug('task %r wakeup by %r', task, timer)
        self.schedule(task, self._task_action_send)

    def _timer_action_timeout(self, timer):
        task = timer.task
        LOG.debug('task %r timeout by %r', task, timer)
        self.schedule(task, self._task_action_timeout)

    def _task_action_timeout(self):
        LOG.debug('throw timeout to task %r', self.current)
        self.current.throw(nio.TaskTimeout())

    def _task_action_cancel(self):
        LOG.debug('throw cancel to task %r', self.current)
        for _ in range(100):
            self.current.throw(nio.TaskCanceled())
        message = 'failed to cancel task {!r}, it may leak resources'\
            .format(self.current)
        LOG.error(message)
        raise RuntimeError(message)

    def _task_action_send(self, value=None):
        LOG.debug('send %r to task %r', value, self.current)
        return self.current.send(value)

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
            except Exception as ex:
                self.stop_task(self.current, error=ex)
                continue
            handler = getattr(self, call.__name__, None)
            if handler is None:
                raise RuntimeError('unknown syscall {}'.format(call.__name__))
            handler(*args)

    def _poll_io(self, time_poll):
        time.sleep(time_poll)

    def nio_sleep(self, seconds):
        self.timer_wheel.start_timer(
            seconds, self._timer_action_wakeup, self.current)

    def nio_set_timeout(self, seconds):
        timer = self.timer_wheel.start_timer(
            seconds, self._timer_action_timeout, self.current)
        user_timeout = nio.Timeout(timer)
        self.schedule(self.current, self._task_action_send, user_timeout)

    def nio_unset_timeout(self, user_timeout):
        timer = user_timeout.__nio_ref_
        if timer is None:
            raise RuntimeError(
                'timeout {!r} not set in kernel'.format(user_timeout))
        self.timer_wheel.stop_timer(timer)
        self.schedule(self.current, self._task_action_send)

    def nio_spawn(self, coro):
        task = self.start_task(coro)
        user_task = nio.Task(task)
        self.schedule(self.current, self._task_action_send, user_task)

    def nio_cancel(self, user_task):
        task = user_task._nio_ref_
        self.schedule(task, self._task_action_cancel)
        self.nio_schedule_wait(task.stop_event)

    def nio_join(self, user_task):
        task = user_task._nio_ref_
        self.nio_schedule_wait(task.stop_event)

    def nio_current_task(self):
        user_task = self.current._nio_ref_
        self.schedule(self.current, self._task_action_send, user_task)

    def nio_schedule_wait(self, user_event):
        event = user_event._nio_ref_
        if event is None:
            event = KernelEvent(user_event)
        if event.is_set:
            self.schedule(self.current, self._task_action_send)
        else:
            event.add_waiting_task(self.current)

    def nio_schedule_notify(self, user_event):
        self.notify(user_event)
        self.schedule(self.current, self._task_action_send)
