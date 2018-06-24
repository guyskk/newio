# Newio: New Async API and Runtime for Python!

[![travis-ci](https://api.travis-ci.org/guyskk/newio.svg)](https://travis-ci.org/guyskk/newio) [![codecov](https://codecov.io/gh/guyskk/newio/branch/master/graph/badge.svg)](https://codecov.io/gh/guyskk/newio)

## Overview

```python
import os
from newio import socket, spawn
from newio_kernel import run

async def echo_handler(client, address):
    async with client:
        while True:
            data = await client.recv(1024)
            if not data:
                break
            await client.sendall(data)

async def start_echo_server(host, port):
    print(f'Echo server pid={os.getpid()}, listening at tcp://{host}:{port}')
    server = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen()
    while True:
        client, address = await server.accept()
        await spawn(echo_handler(client, address))

if __name__ == '__main__':
    run(start_echo_server('127.0.0.1', 25000))
```

## Install

Note: Only support python 3.6+

    pip install 'newio[kernel]'
