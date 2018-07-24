import sys
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

LOG_FMT = '%(levelname)1.1s %(asctime)s %(name)s:%(lineno)-4d %(message)s'
LOG_DATE_FMT = '%H:%M:%S'


def is_terminal():
    return sys.stdout.isatty()


def init_logging(*logger_names, level=logging.INFO, fmt=LOG_FMT, datefmt=LOG_DATE_FMT):
    colored_params = dict(
        fmt=fmt,
        datefmt=datefmt,
        field_styles=DEFAULT_FIELD_STYLES,
        level_styles=DEFAULT_LEVEL_STYLES,
    )
    for name in logger_names:
        logging.getLogger(name).setLevel(level)
    if is_terminal():
        # https://github.com/xolox/python-coloredlogs/issues/54
        coloredlogs.install(**colored_params)
        logging.getLogger().setLevel(logging.WARNING)
        for h in logging.getLogger().handlers:
            h.setLevel(logging.NOTSET)
    else:
        logging.basicConfig(format=fmt, datefmt=datefmt)
