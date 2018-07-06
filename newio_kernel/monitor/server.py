import socket
import json
import logging
import errno
from select import select
from threading import Thread
from concurrent.futures import Future

from newio import spawn
from newio.channel import Channel
from newio_kernel.kernel_api import KernelApiError

LOG = logging.getLogger(__name__)

REQUEST_MAX_SIZE = 1024 * 8
COMMANDS = {
    'get_task_list',
    'get_task',
    'cancel_task',
}


class MonitorServer:
    r'''
    Message Format: bytes end with newline (\n)
    Request(JSON): [command, args, kwargs]
    Response(JSON): [result, error]
    '''

    def __init__(self, kernel_api, host, port):
        self.kernel_api = kernel_api
        self.host = host
        self.port = port
        self._channel = Channel()
        self._agent = None
        self._server = Thread(target=self.monitor_server, daemon=True)
        self._stopped = False
        self._sock_request_stop, self._sock_stop = socket.socketpair()

    async def start(self):
        LOG.debug('[starting] monitor')
        await self._channel.__aenter__()
        self._agent = await spawn(self.monitor_agent())
        self._server.start()
        LOG.debug('[started] monitor')

    async def stop(self):
        LOG.debug('[stopping] monitor')
        self._stopped = True
        self._sock_request_stop.send(b'x')
        self._sock_request_stop.close()
        if self._channel is not None:
            await self._channel.__aexit__()
        if self._agent is not None:
            await self._agent.join()
        LOG.debug('[stopped] monitor')

    def _check_stop(self, sock):
        readables, _, _ = select([sock, self._sock_stop], [], [])
        stop = self._stopped or not readables or self._sock_stop in readables
        if stop:
            self._sock_stop.close()
        return stop

    def monitor_server(self):
        LOG.debug('[running] monitor server')
        server = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind((self.host, self.port))
        except OSError as ex:
            if ex.errno != errno.EADDRINUSE:
                raise
            server.bind((self.host, 0))  # bind random port
        server.listen(1)
        self.host, self.port = server.getsockname()
        print(f'* Monitor server listening at tcp://{self.host}:{self.port}')
        with server:
            while True:
                if self._check_stop(server):
                    break
                try:
                    client, address = server.accept()
                except socket.timeout:
                    continue
                with client:
                    try:
                        self.client_handler(client, address)
                    except Exception:
                        msg = f'Connection #{client.fileno()} crashed:'
                        LOG.warn(msg, exc_info=True)
            LOG.debug('[stopping] monitor server')
        LOG.debug('[stopped] monitor server')

    def client_handler(self, client, address):
        host, port = address
        LOG.info(f'Accept connection #{client.fileno()} from {host}:{port}')
        buffer = b''
        while True:
            if self._check_stop(client):
                break
            data = client.recv(1024)
            if not data:
                break
            buffer += data
            if len(buffer) > REQUEST_MAX_SIZE:
                LOG.warn(f'request too large, size={len(buffer)}, drop it!')
                break
            if b'\n' in buffer:
                self.handle_request(client, buffer)
                buffer = b''
        LOG.info(f'Connection #{client.fileno()} closing')

    def handle_request(self, client, buffer):
        data = buffer.decode('utf-8')
        if not data.strip():
            return
        LOG.debug('Got request: %s', data.strip())
        request = json.loads(data)
        command, args, kwargs = request
        if command not in COMMANDS:
            LOG.warn('unknown command %r', command)
            return
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = {}
        command = getattr(self.kernel_api, command)
        response = Future()
        self._channel.thread_send((response, command, args, kwargs))
        result, error = response.result()
        response = json.dumps([result, error], ensure_ascii=False).strip()
        response_bytes = response.encode('utf-8') + b'\n'
        if error:
            LOG.debug('Send error response')
        else:
            LOG.debug('Send success response')
        client.sendall(response_bytes)

    async def monitor_agent(self):
        LOG.debug('[running] monitor server agent')
        async for response, command, args, kwargs in self._channel:
            result = error = None
            try:
                result = await command(*args, **kwargs)
            except KernelApiError as ex:
                error = str(ex)
            except Exception as ex:
                LOG.error('execute command failed:', exc_info=True)
                error = 'execute command failed: ' + str(ex)
            response.set_result((result, error))
        LOG.debug('[stopping] monitor server agent')
        LOG.debug('[stopped] monitor server agent')
