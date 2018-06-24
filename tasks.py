import os
import sys
import shutil

import logging
import coloredlogs
if os.getenv('DEBUG'):
    fmt = '%(asctime)s %(levelname)s [%(process)d]%(name)s: %(message)s'
    coloredlogs.install(level=logging.DEBUG, fmt=fmt)

from invoke import task
from newio_kernel import run
from tests.echo_server import echo_server


@task(name='echo_server')
def start_echo_server(ctx, host='127.0.0.1', port=25000, debug=False):
    run(echo_server(host, port))


def _build_newio(ctx):
    shutil.copy('tools/setup-newio.py', 'setup.py')
    ctx.run('python setup.py sdist')
    os.remove('setup.py')


def _build_kernel(ctx):
    shutil.copy('tools/setup-kernel.py', 'setup.py')
    ctx.run('python setup.py sdist')
    os.remove('setup.py')


@task
def build(ctx, target='all'):
    if target == 'all':
        _build_newio(ctx)
        _build_kernel(ctx)
    elif target == 'newio':
        _build_newio(ctx)
    elif target == 'kernel':
        _build_kernel(ctx)
    else:
        sys.exit(f'Unknown target {target!r}')
