import time
import logging
from llist import dllist

LOG = logging.getLogger(__name__)


class KernelTimer:
    def __init__(
            self, *, timer_wheel, seconds, action,
            task, deadline, index, rounds):
        self._nio_ref_ = None
        self.timer_wheel = timer_wheel
        self.seconds = seconds
        self.action = action
        self.task = task
        self.deadline = deadline
        self.index = index
        self.rounds = rounds
        self.node = None
        self.is_expired = False

    def stop(self):
        self.timer_wheel._stop_timer(self)

    def __repr__(self):
        return ('<KernelTimer {seconds:.3f}s tick={current_tick}/{index}'
                ' rounds={rounds} task#{task_id}>').format(
            seconds=self.seconds,
            current_tick=self.timer_wheel.current_tick,
            rounds=self.rounds,
            index=self.index,
            task_id=self.task.ident
        )

    def clean(self):
        self.stop()

    def state(self):
        return 'wait_timer'


class TimerWheel:
    '''A timer wheel based on Netty's implemention'''

    def __init__(self, tick_duration=0.010, ticks_per_wheel=1000):
        self.ticks_per_wheel = ticks_per_wheel
        self.tick_duration = tick_duration
        self.wheel = [dllist() for _ in range(ticks_per_wheel)]
        self.current_tick = 0
        self.current_tick_time = time.monotonic()

    def __repr__(self):
        return '<TimerWheel tick#{tick} @{time:.3f}>'.format(
            tick=self.current_tick, time=self.current_tick_time)

    def start_timer(self, seconds, action, task):
        deadline = time.monotonic() + seconds
        ticks = int((deadline - self.current_tick_time) / self.tick_duration)
        rounds, ticks = divmod(ticks, self.ticks_per_wheel)
        index = (self.current_tick + ticks) % self.ticks_per_wheel
        timer = KernelTimer(
            timer_wheel=self, seconds=seconds, action=action, task=task,
            deadline=deadline, index=index, rounds=rounds)
        node = self.wheel[index].append(timer)
        timer.node = node
        LOG.debug('start timer %r', timer)
        return timer

    def _stop_timer(self, timer):
        if not timer.is_expired:
            self.wheel[timer.index].remove(timer.node)

    def check(self):
        duration = time.monotonic() - self.current_tick_time
        ticks = int(duration / self.tick_duration)
        self.current_tick_time += ticks * self.tick_duration
        for _ in range(ticks):
            timers = self.wheel[self.current_tick]
            node = timers.first
            while node:
                timer = node.value
                next_node = node.next
                if timer.rounds <= 0:
                    timer.is_expired = True
                    timers.remove(node)
                    timer.action(timer)
                else:
                    timer.rounds -= 1
                node = next_node
            self.current_tick = (self.current_tick + 1) % self.ticks_per_wheel
