from socket import socketpair
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
        self._s1, self._s2 = socketpair()
        self._s1.setblocking(False)
        self._s2.setblocking(False)
        self._sock_notify_sender = self._sock_receiver_wait = self._s1
        self._sock_notify_receiver = self._sock_sender_wait = self._s2
        self.receiver_wait_fd = self._sock_receiver_wait.fileno()
        self.sender_wait_fd = self._sock_sender_wait.fileno()
        # If bufsize very large, the real socket SO_SNDBUF and SO_RCVBUF
        # may not enough, this may cause BlockingIOError on send.
        self._sock_notify_sender.send(b'1' * self.bufsize)

    def try_recv(self):
        with self._lock:
            if len(self._queue) <= 0:
                if self._closed:
                    raise ChannelClosed()
                return False, None
            self._sock_notify_sender.send(b'1')
            self._sock_receiver_wait.recv(1)
            item = self._queue.popleft()
            return True, item

    def try_send(self, item):
        with self._lock:
            if self._closed:
                raise ChannelClosed()
            if len(self._queue) >= self.bufsize:
                return False
            self._sock_notify_receiver.send(b'1')
            self._sock_sender_wait.recv(1)
            self._queue.append(item)
            return True

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._sock_notify_sender.send(b'x')
            self._sock_notify_receiver.send(b'x')

    def destroy(self):
        self.close()
        with self._lock:
            self._s1.close()
            self._s2.close()

    @property
    def closed(self):
        return self._closed
