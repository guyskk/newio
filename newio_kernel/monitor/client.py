import json
from functools import partial
from socket import create_connection

from .server import COMMANDS


class MonitorApiError(Exception):
    '''Monitor api error'''


class MonitorClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connect()
        for command in COMMANDS:
            fn = partial(self._call, command)
            fn.__name__ = command
            setattr(self, command, fn)

    def connect(self):
        self.sock = create_connection((self.host, self.port))

    def _call(self, command, *args, **kwargs):
        try:
            return self._call_impl(command, *args, **kwargs)
        except MonitorApiError:
            raise
        except Exception:
            self.sock.close()
            self.connect()
            raise

    def _call_impl(self, command, *args, **kwargs):
        request = json.dumps([command, args, kwargs],
                             ensure_ascii=False).strip()
        request_bytes = request.encode('utf-8') + b'\n'
        self.sock.sendall(request_bytes)
        data = self._read_response()
        response = json.loads(data.decode('utf-8'))
        result, error = response
        if error:
            raise MonitorApiError(error)
        return result

    def _read_response(self):
        buffer = b''
        while True:
            data = self.sock.recv(1024)
            if not data:
                break
            buffer += data
            if b'\n' in buffer:
                break
        return buffer

    def close(self):
        self.sock.close()
