'''
A async version for the standard socket library. The entire contents of
stdlib socket are made available here. The socket class is replaced by an
async compatible version.
'''
import socket as std

__all__ = tuple(std.__all__) + ('Socket',)

from socket import *  # noqa: F401,F403
from functools import wraps

from ._socket import Socket
from .api import run_in_thread


@wraps(std.socket)
def socket(*args, **kwargs):
    return Socket(std.socket(*args, **kwargs))


@wraps(std.socketpair)
def socketpair(*args, **kwargs):
    s1, s2 = std.socketpair(*args, **kwargs)
    return Socket(s1), Socket(s2)


@wraps(std.fromfd)
def fromfd(*args, **kwargs):
    return Socket(std.fromfd(*args, **kwargs))


@wraps(std.create_connection)
async def create_connection(*args, **kwargs):
    sock = await run_in_thread(std.create_connection, *args, **kwargs)
    return Socket(sock)


@wraps(std.getaddrinfo)
async def getaddrinfo(*args, **kwargs):
    return await run_in_thread(std.getaddrinfo, *args, **kwargs)


@wraps(std.getfqdn)
async def getfqdn(*args, **kwargs):
    return await run_in_thread(std.getfqdn, *args, **kwargs)


@wraps(std.gethostbyname)
async def gethostbyname(*args, **kwargs):
    return await run_in_thread(std.gethostbyname, *args, **kwargs)


@wraps(std.gethostbyname_ex)
async def gethostbyname_ex(*args, **kwargs):
    return await run_in_thread(std.gethostbyname_ex, *args, **kwargs)


@wraps(std.gethostname)
async def gethostname(*args, **kwargs):
    return await run_in_thread(std.gethostname, *args, **kwargs)


@wraps(std.gethostbyaddr)
async def gethostbyaddr(*args, **kwargs):
    return await run_in_thread(std.gethostbyaddr, *args, **kwargs)


@wraps(std.getnameinfo)
async def getnameinfo(*args, **kwargs):
    return await run_in_thread(std.getnameinfo, *args, **kwargs)
