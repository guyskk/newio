import asyncio
from socket import SOL_SOCKET, SO_ERROR

from .api import wait_read, wait_write, run_in_thread
from .contextlib import contextmanager
from .compat import PY37


try:
    from ssl import SSLWantReadError, SSLWantWriteError
except ImportError:
    # We need these exceptions defined, even if ssl is not available.
    class SSLWantReadError(Exception):
        pass

    class SSLWantWriteError(Exception):
        pass


_loop = asyncio.get_event_loop


class Socket:
    """
    Non-blocking wrapper around a socket object.   The original socket is put
    into a non-blocking mode when it's wrapped.
    """

    def __init__(self, sock):
        self._socket = sock
        self._socket.setblocking(False)
        self._fileno = sock.fileno()
        self._socket_send = sock.send
        self._socket_recv = sock.recv

    def __repr__(self):
        return f'<Socket {self.fileno()}>'

    def __getattr__(self, name):
        return getattr(self._socket, name)

    def fileno(self):
        return self._fileno

    def settimeout(self, seconds):
        raise RuntimeError('Use newio.timeout() to set a timeout')

    def gettimeout(self):
        return None

    def dup(self):
        return type(self)(self._socket.dup())

    @property
    def socket(self):
        """Access to the underlying socket in non-blocking mode"""
        return self._socket

    @contextmanager
    def blocking(self):
        """
        Allow temporary access to the underlying socket in blocking mode
        """
        try:
            self._socket.setblocking(True)
            yield self._socket
        finally:
            self._socket.setblocking(False)

    async def _aio(self, wait_io, async_fn, *args, **kwargs):
        while True:
            try:
                return await async_fn(*args, **kwargs)
            except SSLWantReadError:
                await wait_read(self._fileno)
            except SSLWantWriteError:
                await wait_write(self._fileno)
            except BlockingIOError:
                await wait_io(self._fileno)

    async def _io(self, wait_io, fn, *args, **kwargs):
        while True:
            try:
                return fn(*args, **kwargs)
            except SSLWantReadError:
                await wait_read(self._fileno)
            except SSLWantWriteError:
                await wait_write(self._fileno)
            except BlockingIOError:
                await wait_io(self._fileno)

    async def recv(self, bufsize, flags=0):
        return await self._aio(wait_read, _loop().sock_recv, self._socket, bufsize)

    if PY37:

        async def recv_into(self, buffer, nbytes=0, flags=0):
            return await self._aio(
                wait_read, _loop().sock_recv_into, self._socket, buffer
            )

    else:

        async def recv_into(self, buffer, nbytes=0, flags=0):
            return await self._io(
                wait_read, self._socket.recv_into, buffer, nbytes, flags
            )

    async def send(self, data, flags=0):
        return await self._io(wait_write, self._socket_send, data, flags)

    async def sendall(self, data, flags=0):
        return await self._aio(wait_write, _loop().sock_sendall, self._socket, data)

    if PY37:

        async def sendfile(self, file, offset=0, count=None):
            return await self._aio(
                wait_write,
                _loop().sock_sendfile,
                self._socket,
                file,
                offset=offset,
                count=count,
            )

    else:

        async def sendfile(self, file, offset=0, count=None):
            with self.blocking() as sock:
                return await run_in_thread(
                    sock.sendfile, file, offset=offset, count=count
                )

    async def accept(self):
        client, addr = await self._aio(wait_read, _loop().sock_accept, self._socket)
        return type(self)(client), addr

    async def connect_ex(self, address):
        try:
            await self.connect(address)
            return 0
        except OSError as e:
            return e.errno

    async def connect(self, address):
        try:
            result = self._socket.connect(address)
            if getattr(self, 'do_handshake_on_connect', False):
                await self.do_handshake()
            return result
        except (BlockingIOError, SSLWantWriteError):
            await wait_write(self._fileno)
        except SSLWantReadError:
            await wait_read(self._fileno)
        err = self._socket.getsockopt(SOL_SOCKET, SO_ERROR)
        if err != 0:
            raise OSError(err, 'Connect call failed %s' % (address,))
        if getattr(self, 'do_handshake_on_connect', False):
            await self.do_handshake()

    async def recvfrom(self, buffersize, flags=0):
        return await self._io(wait_read, self._socket.recvfrom, buffersize, flags)

    async def recvfrom_into(self, buffer, bytes=0, flags=0):
        return await self._io(
            wait_read, self._socket.recvfrom_into, buffer, bytes, flags
        )

    async def sendto(self, bytes, flags_or_address, address=None):
        if address:
            flags = flags_or_address
        else:
            address = flags_or_address
            flags = 0
        return await self._io(wait_write, self._socket.sendto, bytes, flags, address)

    async def recvmsg(self, bufsize, ancbufsize=0, flags=0):
        return await self._io(
            wait_read, self._socket.recvmsg, bufsize, ancbufsize, flags
        )

    async def recvmsg_into(self, buffers, ancbufsize=0, flags=0):
        return await self._io(
            wait_read, self._socket.recvmsg_into, buffers, ancbufsize, flags
        )

    async def sendmsg(self, buffers, ancdata=(), flags=0, address=None):
        return await self._io(
            wait_write, self._socket.sendmsg, buffers, ancdata, flags, address
        )

    # Special functions for SSL
    async def do_handshake(self):
        return await self._io(wait_read, self._socket.do_handshake)

    async def close(self):
        if self._socket:
            self._socket.close()
            self._socket = None

    async def shutdown(self, how):
        if self._socket:
            self._socket.shutdown(how)

    async def __aenter__(self):
        self._socket.__enter__()
        return self

    async def __aexit__(self, *args):
        if self._socket:
            self._socket.__exit__(*args)
            self._socket = None
