import socket
from threading import Lock
from collections import deque

from .error import ChannelClosed

MAX_BUFSIZE = 2 ** 32
DEFAULT_BUF_SIZE = 128


class ChannelController:
    def __init__(self, bufsize=DEFAULT_BUF_SIZE):
        if not bufsize or bufsize <= 0:
            bufsize = DEFAULT_BUF_SIZE
        if bufsize > MAX_BUFSIZE:
            raise ValueError('bufsize too large')
        self.bufsize = bufsize
        self._lock = Lock()
        self._queue = deque()
        self._closed = False
        self._s1, self._s2 = socket.socketpair()
        for sock in [self._s1, self._s2]:
            sock.setblocking(False)
        self._sendable = False
        self._recvable = False
        self._sock_set_senable = self._sock_unset_recvable = self._s1
        self._sock_unset_sendable = self._sock_set_recvable = self._s2
        self.receiver_wait_fd = self._s1.fileno()
        self.sender_wait_fd = self._s2.fileno()
        self._set_sendable()

    def _set_sendable(self):
        if self._sendable:
            return
        self._sendable = True
        self._sock_set_senable.send(b'1')

    def _unset_sendable(self):
        if not self._sendable:
            return
        self._sendable = False
        self._sock_unset_sendable.recv(1)

    def _set_recvable(self):
        if self._recvable:
            return
        self._recvable = True
        self._sock_set_recvable.send(b'1')

    def _unset_recvable(self):
        if not self._recvable:
            return
        self._recvable = False
        self._sock_unset_recvable.recv(1)

    def _full(self):
        return len(self._queue) >= self.bufsize

    def _empty(self):
        return len(self._queue) <= 0

    def try_recv(self):
        with self._lock:
            if self._empty():
                if self._closed:
                    raise ChannelClosed()
                self._unset_recvable()
                return False, None
            self._set_sendable()
            item = self._queue.popleft()
            return True, item

    def try_send(self, item):
        with self._lock:
            if self._closed:
                raise ChannelClosed()
            if self._full():
                self._unset_sendable()
                return False
            self._set_recvable()
            self._queue.append(item)
            return True

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._s1.send(b'x')
            self._s2.send(b'x')

    def destroy(self):
        self.close()
        with self._lock:
            self._s1.close()
            self._s2.close()

    @property
    def closed(self):
        with self._lock:
            return self._closed
