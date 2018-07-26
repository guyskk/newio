import logging
from select import select


LOG = logging.getLogger(__name__)


class ThreadBroker:
    def __init__(self, controller):
        self.controller = controller

    def send(self, item):
        while True:
            ok = self.controller.try_send(item)
            if ok:
                break
            select([self.controller.sender_wait_fd], [], [])

    def recv(self):
        while True:
            ok, item = self.controller.try_recv()
            if ok:
                return item
            select([self.controller.receiver_wait_fd], [], [])

    async def join(self):
        """do nothing"""
