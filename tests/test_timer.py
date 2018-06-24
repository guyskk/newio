import logging
from newio_kernel.timer import TimerQueue

LOG = logging.getLogger(__name__)


class MockClock:

    def __init__(self):
        self._t = 10000

    def forward(self, delta):
        self._t += delta

    def __call__(self):
        return self._t


def test_check():
    clock = MockClock()
    timerqueue = TimerQueue(clock=clock)
    logs = []

    def append(timer, *args, **kwargs):
        return logs.append(*args, **kwargs)

    t1 = timerqueue.start_timer(0.5, append, ('#1 expired',))
    t2 = timerqueue.start_timer(1, append, ('#2 expired',))
    t3 = timerqueue.start_timer(100, append, ('#3 expired',))
    t4 = timerqueue.start_timer(1000, append, ('#4 expired',))
    LOG.debug((t1, t2, t3, t4))

    timerqueue.check()
    assert not logs

    clock.forward(1)
    timerqueue.check()
    assert logs == ['#1 expired', '#2 expired']

    for _ in range(98):
        clock.forward(1)
        timerqueue.check()
        assert len(logs) == 2

    clock.forward(1)
    timerqueue.check()
    assert logs == ['#1 expired', '#2 expired', '#3 expired']

    clock.forward(900)
    timerqueue.check()
    assert logs == ['#1 expired', '#2 expired', '#3 expired', '#4 expired']


def test_cancel():
    clock = MockClock()
    timerqueue = TimerQueue(clock=clock)
    logs = []

    def append(timer, *args, **kwargs):
        return logs.append(*args, **kwargs)

    t1 = timerqueue.start_timer(3, append, ('#1 expired',))
    t2 = timerqueue.start_timer(10, append, ('#2 expired',))
    clock.forward(2)
    timerqueue.check()
    t1.cancel()
    t2.cancel()
    clock.forward(2)
    timerqueue.check()
    assert t1.is_canceled
    assert t2.is_canceled
    assert not logs


def test_next_check_interval():
    clock = MockClock()
    timerqueue = TimerQueue(clock=clock)
    logs = []

    def append(timer, *args, **kwargs):
        return logs.append(*args, **kwargs)

    timerqueue.start_timer(1.5, append, ('#1 expired',))
    clock.forward(1)
    timerqueue.check()
    assert timerqueue.next_check_interval() == 0.5
    clock.forward(1)
    timerqueue.check()
    assert logs == ['#1 expired']
    assert timerqueue.next_check_interval() == 1
