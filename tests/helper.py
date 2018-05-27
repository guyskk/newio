import newio.api as nio
import functools


def run_it(coro_func):

    @functools.wraps(coro_func)
    def sync_func(*args, **kwargs):
        return nio.run(coro_func(*args, **kwargs))

    return sync_func
