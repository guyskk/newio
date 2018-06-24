import time
import heapq
import logging
from math import log2

LOG = logging.getLogger(__name__)


class KernelTimer:
    def __init__(self, timerqueue, seconds, deadline, callback, cb_args, cb_kwargs):
        self.timerqueue = timerqueue
        self.seconds = seconds
        self.deadline = deadline
        self.callback = callback
        self.cb_args = cb_args
        self.cb_kwargs = cb_kwargs
        self.bucket = 0
        self.is_expired = False
        self.is_canceled = False
        self._nio_ref_ = None

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __repr__(self):
        if self.is_expired:
            state = 'expired'
        elif self.is_canceled:
            state = 'canceled'
        else:
            remain = max(0, self.deadline - self.timerqueue.clock())
            state = f'remain={remain:.3f}s'
        bucket = '-' if self.bucket < 0 else str(self.bucket)
        return f'<KernelTimer {self.seconds:.3f}s {state} [{bucket}]>'

    def cancel(self):
        self.timerqueue._cancel_timer(self)

    def clean(self):
        self.cancel()

    def state(self):
        return 'wait_timer'


def _compute_buckets(ticks):
    buckets = 0
    while ticks > 0:
        ticks = ticks // 2
        buckets += 1
    return buckets


class TimerQueue:
    '''
    Timers Location:

               <---+-------------------------------->
                   |
               near|              far
                   |
               +---+---+-------+---------------+----+
        bucket | - | 0 |   1   |       2       |
               v   v   v       v               v

         tick  +------------------------------------>
               0   1   2   3   4   5   6   7   8   9

                        +> tick< 1: -1
               bucket = |
                        +> tick>=1: log2(tick)

        Drawing by: http://asciiflow.com/
    '''

    def __init__(self, timeslice=1.0, timespan=300.0, clock=time.monotonic):
        self.timeslice = timeslice
        self.buckets = _compute_buckets(timespan / timeslice)
        self.max_tick = 2 ** self.buckets
        self.clock = clock
        self.tick_time = clock()
        self.tick = 0
        self.near_timers = []
        self.far_timers = [set() for _ in range(self.buckets)]

    def start_timer(self, seconds, callback, cb_args=(), cb_kwargs=None):
        deadline = self.clock() + max(0, seconds)
        if cb_kwargs is None:
            cb_kwargs = {}
        timer = KernelTimer(self, seconds, deadline, callback, cb_args, cb_kwargs)
        self._push_timer(timer)
        return timer

    def _push_timer(self, timer):
        ticks = (timer.deadline - self.tick_time) // self.timeslice
        if ticks < 1:
            bucket = -1
        else:
            bucket = min(self.buckets - 1, int(log2(ticks)))
        timer.bucket = bucket
        LOG.debug('push timer %r', timer)
        if bucket < 0:
            heapq.heappush(self.near_timers, timer)
        else:
            self.far_timers[bucket].add(timer)

    def _cancel_timer(self, timer):
        if not timer.is_expired and not timer.is_canceled:
            LOG.debug('cancel timer %r', timer)
            timer.is_canceled = True
            if timer.bucket >= 0:
                self.far_timers[timer.bucket].remove(timer)

    def next_check_interval(self):
        if self.near_timers:
            return self.near_timers[0].deadline - self.clock()
        else:
            return self.tick_time + self.timeslice - self.clock()

    def _forward_far_buckets(self):
        return [n for n in range(self.buckets) if self.tick % (2**n) == 0]

    def _forward(self):
        ticks = int((self.clock() - self.tick_time) // self.timeslice)
        if ticks > 0:
            self.tick_time += self.timeslice * ticks
            for _ in range(ticks):
                self.tick = (self.tick + 1) % self.max_tick
                for bucket in self._forward_far_buckets():
                    timers = self.far_timers[bucket]
                    if timers:
                        self.far_timers[bucket] = set()
                        for t in timers:
                            self._push_timer(t)

    def check(self):
        self._forward()
        deadline = self.clock()
        while self.near_timers:
            timer = self.near_timers[0]
            if timer.deadline > deadline and not timer.is_canceled:
                break
            timer = heapq.heappop(self.near_timers)
            if not timer.is_expired and not timer.is_canceled:
                LOG.debug('timer %r expired', timer)
                timer.is_expired = True
                timer.callback(timer, *timer.cb_args, **timer.cb_kwargs)
