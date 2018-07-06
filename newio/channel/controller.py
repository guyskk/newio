from socket import socketpair
from threading import Lock

from .error import ChannelClosed

_EMPTY = object()


class ChannelController:
    def __init__(self):
        self._lock = Lock()
        self._item = _EMPTY
        self._closed = False
        self._s1, self._s2 = socketpair()
        self._s1.setblocking(False)
        self._s2.setblocking(False)
        self._sock_notify_sender = self._sock_receiver_wait = self._s1
        self._sock_notify_receiver = self._sock_sender_wait = self._s2
        self.receiver_wait_fd = self._sock_receiver_wait.fileno()
        self.sender_wait_fd = self._sock_sender_wait.fileno()
        self._sock_notify_sender.send(b'1')

    def try_recv(self):
        with self._lock:
            if self._item is _EMPTY:
                if self._closed:
                    raise ChannelClosed()
                return False, None
            self._sock_receiver_wait.recv(1)
            item = self._item
            self._item = _EMPTY
            self._sock_notify_sender.send(b'1')
            return True, item

    def try_send(self, item):
        with self._lock:
            if self._closed:
                raise ChannelClosed()
            if self._item is not _EMPTY:
                return False
            self._sock_sender_wait.recv(1)
            self._item = item
            self._sock_notify_receiver.send(b'1')
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
