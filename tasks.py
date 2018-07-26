from pathlib import Path
from invoke import task

from newio import run
from tests.echo_server import start_echo_server
from benchmark import benchmark_channel


@task
def echo_server(ctx, host='127.0.0.1', port=25000):
    """start echo server"""
    run(start_echo_server(host, port))


@task
def build(ctx):
    """build package"""
    for f in Path('dist').glob('*'):
        f.unlink()
    ctx.run('python setup.py sdist')


@task
def publish(ctx):
    build(ctx)
    ctx.run('twine upload dist/*')


@task
def benchmark(ctx, producer='', consumer=''):
    benchmark_channel.benchmark(producer, (1, 9), consumer, (1, 9))
