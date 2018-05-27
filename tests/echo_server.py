import os
import socket
import newio.api as nio

try:
    from ssl import SSLWantReadError, SSLWantWriteError
    WantRead = (BlockingIOError, InterruptedError, SSLWantReadError)
    WantWrite = (BlockingIOError, InterruptedError, SSLWantWriteError)
except ImportError:    # pragma: no cover
    WantRead = (BlockingIOError, InterruptedError)
    WantWrite = (BlockingIOError, InterruptedError)


def fd(sock):
    return nio.FileDescriptor(sock.fileno())


async def echo_handler(client, address):
    client.setblocking(False)
    try:
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except (OSError, NameError):
        pass
    with client:
        client_fd = fd(client)
        while True:
            try:
                data = client.recv(102400)
            except WantRead:
                await nio.wait_read(client_fd)
                data = client.recv(102400)
            if not data:
                break
            while data:
                try:
                    num = client.send(data)
                except WantWrite:
                    await nio.wait_write(client_fd)
                    num = client.send(data)
                data = data[num:]


async def echo_server(host, port):
    print(f'Echo server pid={os.getpid()}, listening at tcp://{host}:{port}')
    server = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.setblocking(False)
    try:
        server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except (OSError, NameError):
        pass
    server.bind((host, port))
    server.listen()
    server_fd = fd(server)
    while True:
        try:
            client, address = server.accept()
        except WantRead:
            await nio.wait_read(server_fd)
            client, address = server.accept()
        await nio.spawn(echo_handler(client, address))
