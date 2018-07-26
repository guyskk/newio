from array import array

from newio import spawn, sleep, timeout_after, open_nursery, CancelledError
from newio.socket import (
    socket,
    socketpair,
    AF_INET,
    SOCK_STREAM,
    SOL_SOCKET,
    SO_REUSEADDR,
    SOCK_DGRAM,
)
from newio.sync import Event

from .helper import run_it


@run_it
async def test_tcp_echo():
    results = []

    async def server(address):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(address)
        sock.listen(5)
        results.append('accept wait')
        client, addr = await sock.accept()
        results.append('accept done')
        async with open_nursery() as nursery:
            await nursery.spawn(handler, client)
        await sock.close()

    async def handler(client):
        results.append('handler start')
        while True:
            results.append('recv wait')
            data = await client.recv(100)
            if not data:
                break
            results.append(('handler', data))
            await client.sendall(data)
        results.append('handler done')
        await client.close()

    async def client(address):
        results.append('client start')
        sock = socket(AF_INET, SOCK_STREAM)
        await sock.connect(address)
        await sock.send(b'Msg1')
        await sleep(0.1)
        resp = await sock.recv(100)
        results.append(('client', resp))
        await sock.send(b'Msg2')
        await sleep(0.1)
        resp = await sock.recv(100)
        results.append(('client', resp))
        results.append('client close')
        await sock.close()

    async with open_nursery() as nursery:
        await nursery.spawn(server, ('localhost', 25000))
        await nursery.spawn(client, ('localhost', 25000))

    assert results == [
        'accept wait',
        'client start',
        'accept done',
        'handler start',
        'recv wait',
        ('handler', b'Msg1'),
        'recv wait',
        ('client', b'Msg1'),
        ('handler', b'Msg2'),
        'recv wait',
        ('client', b'Msg2'),
        'client close',
        'handler done',
    ]


@run_it
async def test_udp_echo():
    results = []

    async def server(address):
        sock = socket(AF_INET, SOCK_DGRAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(address)
        results.append('recvfrom wait')
        data, addr = await sock.recvfrom(8192)
        results.append(('server', data))
        await sock.sendto(data, addr)
        await sock.close()
        results.append('server close')

    async def client(address):
        results.append('client start')
        sock = socket(AF_INET, SOCK_DGRAM)
        results.append('client send')
        await sock.sendto(b'Msg1', address)
        data, addr = await sock.recvfrom(8192)
        results.append(('client', data))
        await sock.close()
        results.append('client close')

    async with open_nursery() as nursery:
        await nursery.spawn(server, ('', 25000))
        await nursery.spawn(client, ('localhost', 25000))

    assert results == [
        'recvfrom wait',
        'client start',
        'client send',
        ('server', b'Msg1'),
        'server close',
        ('client', b'Msg1'),
        'client close',
    ]


@run_it
async def test_accept_timeout():
    results = []

    async def server(address):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(address)
        sock.listen(1)
        results.append('accept wait')
        async with timeout_after(0.1) as is_timeout:
            client, addr = await sock.accept()
            results.append('not here')
        if is_timeout:
            results.append('accept timeout')
        await sock.close()

    await server(('', 25000))

    assert results == ['accept wait', 'accept timeout']


@run_it
async def test_accept_cancel():
    results = []

    async def server(address):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(address)
        sock.listen(1)
        results.append('accept wait')
        try:
            client, addr = await sock.accept()
            results.append('not here')
        except CancelledError:
            results.append('accept cancel')
        await sock.close()

    task = await spawn(server, ('', 25000))
    await sleep(0.1)
    await task.cancel()

    assert results == ['accept wait', 'accept cancel']


@run_it
async def test_recv_timeout():
    results = []

    async def server(address, accepting_event):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(address)
        sock.listen(1)
        results.append('accept wait')
        await accepting_event.set()
        client, addr = await sock.accept()
        results.append('recv wait')
        async with timeout_after(0.1) as is_timeout:
            await client.recv(8192)
            results.append('not here')
        if is_timeout:
            results.append('recv timeout')
        await client.close()
        await sock.close()

    async def canceller():
        accepting_event = Event()
        task = await spawn(server, ('', 25000), accepting_event)
        await accepting_event.wait()
        sock = socket(AF_INET, SOCK_STREAM)
        results.append('client connect')
        await sock.connect(('localhost', 25000))
        await sleep(0.2)
        await sock.close()
        results.append('client done')
        await task.join()

    await canceller()

    assert results == [
        'accept wait',
        'client connect',
        'recv wait',
        'recv timeout',
        'client done',
    ]


@run_it
async def test_recv_cancel():
    results = []

    async def server(address, accepting_event):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(address)
        sock.listen(1)
        results.append('accept wait')
        await accepting_event.set()
        client, addr = await sock.accept()
        results.append('recv wait')
        try:
            await client.recv(8192)
            results.append('not here')
        except CancelledError:
            results.append('recv cancel')
        await client.close()
        await sock.close()

    async def canceller():
        accepting_event = Event()
        task = await spawn(server, ('', 25000), accepting_event)
        await accepting_event.wait()
        sock = socket(AF_INET, SOCK_STREAM)
        results.append('client connect')
        await sock.connect(('localhost', 25000))
        await sleep(0.2)
        await task.cancel()
        await sock.close()
        results.append('client done')

    await canceller()

    assert results == [
        'accept wait',
        'client connect',
        'recv wait',
        'recv cancel',
        'client done',
    ]


@run_it
async def test_recvfrom_timeout():
    results = []

    async def server(address):
        sock = socket(AF_INET, SOCK_DGRAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(address)
        results.append('recvfrom wait')
        async with timeout_after(0.1) as is_timeout:
            await sock.recvfrom(8192)
            results.append('not here')
        if is_timeout:
            results.append('recvfrom timeout')
        await sock.close()

    async def canceller():
        t = await spawn(server, ('', 25000))
        await sleep(0.2)
        results.append('client done')
        await t.join()

    await canceller()

    assert results == ['recvfrom wait', 'recvfrom timeout', 'client done']


@run_it
async def test_recvfrom_cancel():
    results = []

    async def server(address):
        sock = socket(AF_INET, SOCK_DGRAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(address)
        results.append('recvfrom wait')
        try:
            await sock.recvfrom(8192)
            results.append('not here')
        except CancelledError:
            results.append('recvfrom cancel')
        await sock.close()

    async def canceller():
        task = await spawn(server, ('', 25000))
        await sleep(0.2)
        await task.cancel()
        results.append('client done')

    await canceller()

    assert results == ['recvfrom wait', 'recvfrom cancel', 'client done']


@run_it
async def test_buffer_into():

    results = []

    async def sender(s1):
        a = array('i', range(1000000))
        data = memoryview(a).cast('B')
        await s1.sendall(data)

    async def receiver(s2):
        a = array('i', (0 for n in range(1000000)))
        view = memoryview(a).cast('B')
        total = 0
        while view:
            nrecv = await s2.recv_into(view)
            if not nrecv:
                break
            total += nrecv
            view = view[nrecv:]

        results.append(a)

    s1, s2 = socketpair()

    async with open_nursery() as nursery:
        await nursery.spawn(sender, s1)
        await nursery.spawn(receiver, s2)

    await s1.close()
    await s2.close()

    assert all(n == x for n, x in enumerate(results[0]))
