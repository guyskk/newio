import socket
import newio as nio

from .helper import run_it


@run_it
async def test_io():
    client, server = socket.socketpair()

    async def server_task():
        fd = server.fileno()
        await nio.wait_read(fd)
        char = server.recv(1)
        await nio.wait_write(fd)
        server.send(char)

    async def client_task(char):
        fd = client.fileno()
        await nio.wait_write(fd)
        client.send(char)
        await nio.wait_read(fd)
        return client.recv(1)

    task1 = await nio.spawn(server_task())
    task2 = await nio.spawn(client_task(b'x'))
    await task1.join()
    await task2.join()
    assert task2.result == b'x'
