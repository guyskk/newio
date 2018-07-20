import os
import sys
import shutil
import logging
import coloredlogs
from pathlib import Path
from invoke import task

import _newio
from newio_kernel import run
from tests.echo_server import start_echo_server
from benchmark import benchmark_channel


LOG_FMT = (
    '%(levelname)1.1s %(asctime)s P%(process)-5s %(name)s:%(lineno)-4d %(message)s'
)


def init_logging(debug=False):
    if debug:
        coloredlogs.install(level=logging.DEBUG, fmt=LOG_FMT)


@task
def echo_server(ctx, host='127.0.0.1', port=25000, debug=False):
    """start echo server"""
    init_logging(debug)
    run(start_echo_server(host, port))


def _get_targets(target):
    if target == 'all':
        return ['newio', 'newio-kernel']
    if target == 'newio':
        return ['newio']
    if target == 'kernel':
        return ['newio', 'newio-kernel']
    sys.exit(f'Unknown build target {target!r}')


@task
def build(ctx, target='all'):
    """build package"""
    for f in Path('dist').glob('*'):
        f.unlink()
    for name in _get_targets(target):
        shutil.copy(f'tools/setup-{name}.py', 'setup.py')
        ctx.run('python setup.py sdist')
        os.remove('setup.py')


@task
def publish(ctx, target='all'):
    """publish package"""
    build(ctx, target=target)
    version = _newio.__version__
    for pkg in _get_targets(target):
        cmd = f'twine upload dist/{pkg}-{version}.tar.gz'
        ctx.run(cmd)


@task
def version(ctx, bump='+'):
    """bump version"""
    origin = _newio.__version__
    major, minor, patch = map(int, origin.split('.'))
    if bump == '+':
        patch += 1
    elif bump == '++':
        minor += 1
        patch = 0
    elif bump == '+++':
        major += 1
        minor = 0
        patch = 0
    else:
        sys.exit(f'Unknown version bump {bump!r}, choices: +, ++, +++')
    version = f'{major}.{minor}.{patch}'
    msg = f'Bump version: {origin} -> {version}'
    print(msg)
    with open('_newio/version.txt', 'w') as f:
        f.write(version + '\n')
    ctx.run(f"git commit -a -m '{msg}'")
    ctx.run(f'git tag {version}')


@task
def benchmark(ctx, producer='', consumer='', debug=False):
    init_logging(debug)
    benchmark_channel.benchmark(producer, (1, 9), consumer, (1, 9))
