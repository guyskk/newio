import os
import sys
import shutil
from pathlib import Path
from invoke import task

from newio import run
from tests.echo_server import start_echo_server
from benchmark import benchmark_channel


@task
def echo_server(ctx, host='127.0.0.1', port=25000):
    """start echo server"""
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
def benchmark(ctx, producer='', consumer=''):
    benchmark_channel.benchmark(producer, (1, 9), consumer, (1, 9))
