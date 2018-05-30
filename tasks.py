import os
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
