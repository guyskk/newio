import os
import sys
import shutil
import logging
import coloredlogs
from invoke import task

import _newio
from newio_kernel import run
from tests.echo_server import start_echo_server

LOG_FMT = '%(asctime)s %(levelname)s [%(process)d]%(name)s: %(message)s'


@task
def echo_server(ctx, host='127.0.0.1', port=25000, debug=False):
    if debug:
        coloredlogs.install(level=logging.DEBUG, fmt=LOG_FMT)
    run(start_echo_server(host, port))


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
        sys.exit(f'Unknown build target {target!r}')


@task
def publish(ctx, target='all'):
    build(ctx, target=target)
    version = _newio.__version__
    if target == 'all':
        pkgs = ['newio', 'newio-kernel']
    else:
        pkgs = [target]
    for pkg in pkgs:
        cmd = f'twine upload {pkg}-{version}.tar.gz'
        ctx.run(cmd)


@task
def version(ctx, bump='+'):
    origin = _newio.__version__
    major, minor, patch = map(int, origin.split('.'))
    if bump == '+':
        patch += 1
    elif bump == '++':
        minor += 1
    elif bump == '+++':
        major += 1
    else:
        sys.exit(f'Unknown version bump {bump!r}, choices: +, ++, +++')
    version = f'{major}.{minor}.{patch}'
    print(f'bump version {origin} -> {version}')
    with open('_newio/version.txt', 'w') as f:
        f.write(version)
