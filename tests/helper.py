import functools
from newio_kernel import run


def run_it(coro_func):

    @functools.wraps(coro_func)
    def sync_func(*args, **kwargs):
        return run(coro_func(*args, **kwargs), timeout=3)

    return sync_func
