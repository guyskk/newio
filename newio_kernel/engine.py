import logging
from collections import deque
from newio.syscall import TaskCanceled, TaskTimeout

LOG = logging.getLogger(__name__)


class Command:

    @staticmethod
    def send(task, value=None):
        return task.send(value)

    @staticmethod
    def throw(task, error):
        return task.throw(error)

    @staticmethod
    def timeout(task, timer):
        return task.throw(TaskTimeout(timer))

    @staticmethod
    def cancel(task):
        # task may await in finally block, try best effort to unwind stack
        for _ in range(1000):
            task.throw(TaskCanceled())
        message = f'failed to cancel task {task!r}, it may leak resources'
        LOG.error(message + ':\n' + task.format_stack())
        raise RuntimeError(message)


class Engine:

    def __init__(self, syscall_handler):
        self._syscall_handler = syscall_handler
        self._tasks = deque()

    def schedule(self, task, command, *value):
        self._tasks.append((task, command, *value))

    def schedule_first(self, task, command, *value):
        self._tasks.appendleft((task, command, *value))

    def execute(self, task, command, *value):
        try:
            call, *args = command(task, *value)
        except StopIteration as stoped:
            task.stop(result=stoped.value)
        except (TaskCanceled, Exception) as ex:
            task.stop(error=ex)
        except BaseException as ex:
            task.stop(error=ex)
            raise
        else:
            self._syscall_handler(task, call, *args)

    def force_cancel(self, task):
        '''cancel a task, ignore any exception here'''
        try:
            Command.cancel(task)
        except StopIteration as stoped:
            task.stop(result=stoped.value)
        except BaseException as ex:
            task.stop(error=ex)

    def run(self):
        while self._tasks:
            task, command, *value = self._tasks.popleft()
            if not task.is_alive:
                LOG.debug('task %r not alive', task)
                continue
            self.execute(task, command, *value)
