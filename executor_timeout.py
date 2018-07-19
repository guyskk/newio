import time
from newio import run_in_thread, spawn, timeout_after, current_task
from newio_kernel import run

import logging
import coloredlogs


GREEN = 41
BLUE = 75
PURPLE = 140
RED = 9
GRAY = 240

DEFAULT_FIELD_STYLES = {
    'asctime': {'color': GREEN},
    'hostname': {'color': PURPLE},
    'levelname': {'color': GRAY, 'bold': True},
    'name': {'color': BLUE},
    'process': {'color': PURPLE},
    'programname': {'color': BLUE},
}

DEFAULT_LEVEL_STYLES = {
    'spam': {'color': GREEN, 'faint': True},
    'success': {'color': GREEN, 'bold': True},
    'verbose': {'color': GREEN},
    'debug': {'color': GREEN},
    'info': {},
    'notice': {},
    'error': {'color': RED},
    'critical': {'color': RED},
    'warning': {'color': 'yellow'},
}

LOGGER_FORMAT = '%(levelname)1.1s %(asctime)s %(name)s:%(lineno)-4d %(message)s'
LOGGER_DATEFMT = '%H:%M:%S'


def config_logging(level, logger_colored=True):
    fmt = LOGGER_FORMAT
    datefmt = LOGGER_DATEFMT
    colored_params = dict(
        fmt=fmt,
        datefmt=datefmt,
        field_styles=DEFAULT_FIELD_STYLES,
        level_styles=DEFAULT_LEVEL_STYLES,
    )
    for name in ['newio', 'newio_kernel']:
        logging.getLogger(name).setLevel(level)
    if logger_colored:
        # https://github.com/xolox/python-coloredlogs/issues/54
        coloredlogs.install(**colored_params)
        logging.getLogger().setLevel(logging.WARNING)
        for h in logging.getLogger().handlers:
            h.setLevel(logging.NOTSET)
    else:
        logging.basicConfig(format=fmt, datefmt=datefmt)


# async def fn(task):
#     for i in range(3):
#         print(f'T{task} -- {i}')
#         async with timeout_after(0.1):
#             time.sleep(1)
#             await run_in_thread(time.sleep, 0.01)


# async def main():
#     config_logging(level="DEBUG")
#     async with open_nursery() as nursery:
#         for i in range(2):
#             await nursery.spawn(fn(i))

async def fn():
    await run_in_thread(lambda: 'OK')
    async with timeout_after(0.01) as is_timeout:
        await run_in_thread(lambda: 'EXPIRED')
    assert is_timeout
    ret = await run_in_thread(lambda: 'OK')
    print(ret)
    assert ret == 'EXPIRED'


async def main():
    config_logging(level='DEBUG')
    task = await spawn(fn())
    await run_in_thread(lambda: 'OK')
    await current_task()
    time.sleep(1)
    await task.join()


if __name__ == '__main__':
    run(main())
