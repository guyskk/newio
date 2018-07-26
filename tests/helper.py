import functools
from newio import Runner

run = Runner(debug=True)


def run_it(coro_func):
    @functools.wraps(coro_func)
    def sync_func(*args, **kwargs):
        return run(coro_func(*args, **kwargs))

    return sync_func
