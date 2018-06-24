import sys
import re
from terminaltables import AsciiTable

from newio_kernel.kernel import MONITOR_DEFAULT_HOST, MONITOR_DEFAULT_PORT
from .client import MonitorClient, MonitorApiError

try:
    # provide elaborate line editing and history features
    import readline  # noqa
except ImportError:
    pass

SPACE = re.compile(r'\s+')


def sout(text):
    sys.stdout.write(text)
    sys.stdout.flush()


class MonitorShell:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = None

    def handler(self, line):
        line = line.strip()
        if not line:
            return
        tokens = SPACE.split(line)
        command = tokens[0]
        args = list(tokens[1:])
        kwargs = {}
        command_handler = getattr(self, 'command_' + command, None)
        if command_handler is None:
            sout(f'unknown command {command}\n')
            return
        try:
            command_handler(*args, **kwargs)
        except (TypeError, MonitorApiError) as ex:
            sout(str(ex) + '\n')

    def main(self):
        sout(f'Monitor connect to {self.host}:{self.port}\n')
        self.client = MonitorClient(self.host, self.port)
        num_tasks = len(self.client.get_task_list())
        sout(f'{num_tasks} tasks running, type help for commands!\n')
        while True:
            try:
                line = input('Monitor> ')
                self.handler(line)
            except EOFError:
                break

    def command_ps(self, ident=None):
        if ident is None:
            return self._show_task_list()
        else:
            return self._show_task(int(ident))

    def command_kill(self, ident):
        return self._cancel_task(int(ident))

    def command_help(self):
        self._show_help()

    def _show_help(self):
        sout(
            '    ps           : Show task table\n'
            '    ps <ID>      : Show detail of an task\n'
            '    kill <ID>    : Cancel an task\n'
            '    Ctrl+C       : Leave the monitor\n'
        )

    def _show_task_list(self):
        tasks = self.client.get_task_list()
        data = [('ID', 'Name', 'State', 'Waiting')]
        for task in tasks:
            data.append((
                task['ident'],
                task['name'],
                task['state'],
                task['waiting'] or '',
            ))
        table = AsciiTable(data)
        sout(table.table)
        sout('\n')

    def _show_task(self, ident):
        task = self.client.get_task(ident)
        for k in ['ident', 'name', 'state', 'waiting']:
            sout(f'{k:>8}: {task[k]}\n')
        task_stack = task['stack'].strip()
        if task_stack:
            sout('-' * 60 + '\n')
            sout(task_stack + '\n')
            sout('-' * 60 + '\n')

    def _cancel_task(self, ident):
        task = self.client.cancel_task(ident)
        error_type = task['error_type']
        if error_type and error_type != 'TaskCanceled':
            sout(f'task #{ident} not well canceled, it may leak resources\n')
        else:
            sout(f'task #{ident} successfully canceled\n')
        task_stack = task['stack'].strip()
        if task_stack:
            sout('-' * 60 + '\n')
            sout(task_stack + '\n')
            sout('-' * 60 + '\n')


def main():
    if len(sys.argv) > 2:
        print('Usage: python -m newio_kernel.monitor <HOST:PORT>')
        return
    if len(sys.argv) == 2:
        host_port = sys.argv[1]
        if host_port in ['-h', '--help']:
            print('Usage: python -m newio_kernel.monitor <HOST:PORT>')
            return
        try:
            host, port = host_port.split(':')
            port = int(port)
        except Exception:
            print(f'Invalid host:port {host_port!r}')
            return
    else:
        host, port = MONITOR_DEFAULT_HOST, MONITOR_DEFAULT_PORT
    shell = MonitorShell(host, port)
    try:
        shell.main()
    except KeyboardInterrupt:
        pass
    except ConnectionError as ex:
        print(ex)
