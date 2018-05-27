from invoke import task
import logging

import newio.api as nio
from tests.echo_server import echo_server


@task(name='echo_server')
def start_echo_server(ctx, host='127.0.0.1', port=25000, debug=False):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    nio.run(echo_server(host, port))
