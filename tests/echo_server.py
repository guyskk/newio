import os
from newio import socket, spawn


def tcp_nodelay(sock):
    try:
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except (OSError, NameError):
        pass


async def echo_handler(client, address):
    tcp_nodelay(client)
    async with client:
        while True:
            data = await client.recv(102400)
            if not data:
                break
            await client.sendall(data)


async def echo_server(host, port):
    print(f'Echo server pid={os.getpid()}, listening at tcp://{host}:{port}')
    server = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_nodelay(server)
    server.bind((host, port))
    server.listen()
    while True:
        client, address = await server.accept()
        await spawn(echo_handler(client, address))
