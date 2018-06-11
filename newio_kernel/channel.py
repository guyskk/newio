import logging
from queue import Queue as ThreadQueue
from queue import Empty as QUEUE_EMPTY
from newio.socket import socketpair

LOG = logging.getLogger(__name__)


class Channel:

    def __init__(self):
        self._notify_sender, self._notify_reader = socketpair()
        self._queue = ThreadQueue()

    def send(self, command, task, *value):
        self._queue.put((command, task, *value))
        with self._notify_sender.blocking() as sock:
            sock.sendall(b'\1')

    async def consumer(self):
        while True:
            await self._notify_reader.recv(32)
            while True:
                try:
                    command, task, *value = self._queue.get_nowait()
                except QUEUE_EMPTY:
                    break
                else:
                    if task.is_alive:
                        LOG.debug('task %r wakeup by executor', task)
                        command(task, *value)

    def close(self):
        with self._notify_sender.blocking() as sock:
            sock.close()
        with self._notify_reader.blocking() as sock:
            sock.close()
