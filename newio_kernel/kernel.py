'''
Newio Kernel
'''
import os
import time
import logging
from inspect import isawaitable

from pyllist import dllist

from newio.syscall import Task as UserTask
from newio.syscall import Timer as UserTimer
from newio.syscall import Lounge as UserLounge
from newio.syscall import TaskCanceled

from .timer import TimerQueue
from .executor import Executor
from .selector import Selector
from .lounge import KernelLounge
from .engine import Engine, Command
from .helper import format_task_stack
from .kernel_api import KernelApi
from .monitor import MonitorServer

LOG = logging.getLogger(__name__)
DEFAULT_MAX_NUM_PROCESS = os.cpu_count()
DEFAULT_MAX_NUM_THREAD = DEFAULT_MAX_NUM_PROCESS * 16

MONITOR_DEFAULT_HOST = '127.0.0.1'
MONITOR_DEFAULT_PORT = 49802


class Runner:
    '''coroutine runner'''

    def __init__(self, *args, **kwargs):
        self.k_args = args
        self.k_kwargs = kwargs

    def __call__(self, *args, **kwargs):
        kernel = Kernel(*self.k_args, **self.k_kwargs)
        return kernel.run(*args, **kwargs)


def _get_task_name(coro):
    name = getattr(coro, '__qualname__', None)
    if name is None:
        name = getattr(coro, '__name__', None)
    if name is None:
        return str(coro)
    return name


class KernelTask:
    def __init__(self, kernel, coro, *, ident):
        self._nio_ref_ = None
        self._kernel = kernel
        self.coro = coro
        self.send = coro.send
        self.throw = coro.throw
        self.ident = ident
        self.name = _get_task_name(coro)
        self.is_alive = True
        self.node = None
        self.stop_lounge = KernelLounge(UserLounge())
        self.error = None
        self.result = None
        self.waiting = None

    @property
    def state(self):
        if self.is_alive:
            if self.waiting is None:
                state = 'ready'
            else:
                state = self.waiting.state()
        elif self.error:
            state = 'error'
        else:
            state = 'stoped'
        return state

    def __repr__(self):
        return '<KernelTask#{} {} @{}>'.format(
            self.ident, self.name, self.state)

    def clean_waiting(self):
        if self.waiting:
            self.waiting.clean()
            self.waiting = None

    def stop(self, *args, **kwargs):
        self._kernel.stop_task(self, *args, **kwargs)

    def format_stack(self):
        return format_task_stack(self)


class Kernel:
    def __init__(
        self,
        max_num_thread=DEFAULT_MAX_NUM_THREAD,
        max_num_process=DEFAULT_MAX_NUM_PROCESS,
        monitor_enable=False,
        monitor_host=MONITOR_DEFAULT_HOST,
        monitor_port=MONITOR_DEFAULT_PORT,
    ):
        self.next_task_id = 0
        self.tasks = dllist()
        self.selector = Selector()
        self.clock = time.monotonic
        self.timer_queue = TimerQueue(clock=self.clock)
        self.engine = Engine(self.syscall_handler)
        self.executor = Executor(
            handler=self._executor_handler,
            max_num_thread=max_num_thread,
            max_num_process=max_num_process)
        self.main_task = None
        self.kernel_tasks = set()
        self.api = KernelApi(self)
        self.monitor_enable = monitor_enable
        self.monitor_host = monitor_host
        self.monitor_port = monitor_port
        self.monitor_server = None

    async def _start_monitor_server(self):
        monitor_enable = os.getenv('NEWIO_KERNEL_MONITOR')
        monitor_host = os.getenv('NEWIO_KERNEL_MONITOR_HOST')
        monitor_port = os.getenv('NEWIO_KERNEL_MONITOR_PORT')
        if monitor_enable == '0':
            self.monitor_enable = False
        elif monitor_enable == '1':
            self.monitor_enable = True
        if monitor_host:
            self.monitor_host = monitor_host
        if monitor_port:
            self.monitor_port = monitor_port
        if self.monitor_enable:
            self.monitor_server = MonitorServer(
                self.api, host=self.monitor_host, port=self.monitor_port)
            await self.monitor_server.start()

    def run(self, coro, timeout=None):
        self.main_task = self.start_task(self.kernel_main(coro))
        if timeout is not None:
            self.timer_queue.start_timer(
                timeout, self._timer_action_cancel, (self.main_task,))
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
        self.kernel_tasks.update(set(self.tasks))
        try:
            await self.executor.start()
            await self._start_monitor_server()
            self.kernel_tasks.update(set(self.tasks))
            return await coro
        finally:
            # when main task exiting, normally cancel all subtasks
            LOG.debug('cancel all tasks, len(tasks)=%d', len(self.tasks))
            tasks = set(self.tasks) - self.kernel_tasks
            for task in tasks:
                user_task = task._nio_ref_
                if user_task.is_alive:
                    await user_task.cancel()
                await user_task.join()
            if self.monitor_server:
                await self.monitor_server.stop()
            await self.executor.stop()
            if len(self.tasks) > 1:
                LOG.error('Unfinished tasks: %r', self.tasks)

    def close(self, wait=True):
        '''normal exit'''
        self.selector.close()
        self.executor.shutdown(wait=wait)

    def shutdown(self):
        '''force exit'''
        # force cancel all tasks
        while self.tasks:
            task = self.tasks.last.value
            if task.is_alive:
                self.engine.force_cancel(task)
        self.close(wait=False)

    def _run(self):
        while self.tasks:
            self.timer_queue.check()
            self.engine.run()
            if not self.tasks:
                break
            time_poll = self.timer_queue.next_check_interval()
            for task in self.selector.poll(time_poll):
                self.engine.execute(task, Command.send)
            self.engine.run()

    def start_task(self, coro):
        # create
        task = KernelTask(self, coro, ident=self.next_task_id)
        LOG.debug('start task %r', task)
        self.next_task_id = self.next_task_id + 1
        # register
        node = self.tasks.append(task)
        task.node = node
        # schedule
        self.engine.schedule(task, Command.send)
        return task

    def stop_task(self, task, *, result=None, error=None):
        if error:
            if isinstance(error, TaskCanceled):
                LOG.debug('task %r canceled', task)
            else:
                LOG.info('task %r crashed:', task, exc_info=error)
        else:
            LOG.debug('stop task %s', task)
        task.is_alive = False
        task.result = result
        task.error = error
        # cleanup
        task.clean_waiting()
        for wakeup_task in task.stop_lounge.wake_all():
            self.engine.schedule(wakeup_task, Command.send)
        # unregister
        self.tasks.remove(task.node)

    def _executor_handler(self, task, result, error):
        if task.is_alive:
            LOG.debug('task %r wakeup by executor', task)
            if error is None:
                self.engine.execute(task, Command.send, result)
            else:
                self.engine.execute(task, Command.throw, error)
            task.clean_waiting()

    def _timer_action_wakeup(self, timer, task):
        LOG.debug('task %r wakeup by timer %r', task, timer)
        if task.is_alive:
            task.clean_waiting()
            self.engine.execute(task, Command.send)

    def _timer_action_timeout(self, timer, task):
        LOG.debug('task %r timeout by timer %r', task, timer)
        if task.is_alive:
            self.engine.execute(task, Command.timeout, timer._nio_ref_)

    def _timer_action_cancel(self, timer, task):
        LOG.debug('task %r cancel by timer %r', task, timer)
        if task.is_alive:
            self.engine.execute(task, Command.cancel)

    def syscall_handler(self, current, call, *args):
        handler = getattr(self, call.__name__, None)
        if handler is None:
            raise RuntimeError('unknown syscall {}'.format(call.__name__))
        handler(current, *args)

    def nio_sleep(self, current, seconds):
        timer = self.timer_queue.start_timer(
            seconds, self._timer_action_wakeup, (current,))
        current.clean_waiting()
        current.waiting = timer

    def nio_timeout_after(self, current, seconds):
        timer = self.timer_queue.start_timer(
            seconds, self._timer_action_timeout, (current,))
        user_timer = UserTimer(timer)
        self.engine.schedule(current, Command.send, user_timer)

    def nio_unset_timer(self, current, user_timer):
        timer = user_timer._nio_ref_
        if timer is None:
            raise RuntimeError(f'timer {user_timer!r} not set in kernel')
        timer.cancel()
        self.engine.schedule(current, Command.send)

    def nio_spawn(self, current, coro, cancel_after=None):
        if not isawaitable(coro):
            error = ValueError(f'can not spawn {coro}, it not awaitable!')
            self.engine.schedule(current, Command.throw, error)
            return
        task = self.start_task(coro)
        user_task = UserTask(task)
        self.engine.schedule(current, Command.send, user_task)

    def nio_cancel(self, current, user_task):
        task = user_task._nio_ref_
        if task.is_alive:
            self.engine.schedule(task, Command.cancel)
        self.engine.schedule_first(current, Command.send)

    def nio_join(self, current, user_task):
        task = user_task._nio_ref_
        if task.is_alive:
            self.nio_lounge_wait(current, task.stop_lounge._nio_ref_)
        else:
            self.engine.schedule(current, Command.send)

    def nio_current_task(self, current):
        user_task = current._nio_ref_
        self.engine.schedule(current, Command.send, user_task)

    def nio_lounge_wait(self, current, user_lounge):
        lounge = KernelLounge.of(user_lounge)
        waiter = lounge.add_waiter(current)
        current.clean_waiting()
        current.waiting = waiter

    def nio_lounge_wake(self, current, user_lounge, n):
        lounge = KernelLounge.of(user_lounge)
        if n == UserLounge.WAKE_ALL:
            wakeup_tasks = lounge.wake_all()
        else:
            wakeup_tasks = lounge.wake(n)
        for task in wakeup_tasks:
            task.clean_waiting()
            self.engine.schedule(task, Command.send)
        self.engine.schedule(current, Command.send)

    def nio_wait_read(self, current, user_fd):
        fd = self.selector.register_read(current, user_fd)
        current.waiting = fd

    def nio_wait_write(self, current, user_fd):
        fd = self.selector.register_write(current, user_fd)
        current.waiting = fd

    def nio_run_in_thread(self, current, fn, args, kwargs):
        fut = self.executor.run_in_thread(current, fn, args, kwargs)
        current.clean_waiting()
        current.waiting = fut

    def nio_run_in_process(self, current, fn, args, kwargs):
        fut = self.executor.run_in_process(current, fn, args, kwargs)
        current.clean_waiting()
        current.waiting = fut

    def nio_run_in_asyncio(self, current, coro):
        fut = self.executor.run_in_asyncio(current, coro)
        current.clean_waiting()
        current.waiting = fut
