import socket
import json
import logging
from threading import Thread
from concurrent.futures import Future

from newio import spawn
from newio.channel import ThreadChannel
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
        self._channel = ThreadChannel()
        self._agent = None
        self._server = Thread(target=self.monitor_server, daemon=True)
        self._stoped = False

    async def start(self):
        LOG.debug('Monitor server starting')
        await self._channel.__aenter__()
        self._agent = await spawn(self.monitor_agent())
        self._server.start()

    async def stop(self):
        LOG.debug('Monitor server stopping')
        self._stoped = True
        if self._channel is not None:
            await self._channel.__aexit__()
        if self._agent is not None:
            await self._agent.join()

    def monitor_server(self):
        server = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.settimeout(0.2)
        server.bind((self.host, self.port))
        server.listen(1)
        host, port = server.getsockname()
        print(f'Monitor server listening at tcp://{host}:{port}')
        with server:
            while True:
                if self._stoped:
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
        LOG.info('Monitor server stoped')

    def client_handler(self, client, address):
        host, port = address
        LOG.info(f'Accept connection #{client.fileno()} from {host}:{port}')
        buffer = b''
        while True:
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
        LOG.debug('Send response: %s', response)
        client.sendall(response_bytes)

    async def monitor_agent(self):
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
