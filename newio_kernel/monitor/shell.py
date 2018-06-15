import sys
import re
import colorama
from colorama import Fore
from terminaltables import AsciiTable

from newio_kernel.kernel import MONITOR_HOST, MONITOR_PORT
from .client import MonitorClient, MonitorApiError

try:
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
            sout(f'Unknown command {command}\n')
            return
        try:
            command_handler(*args, **kwargs)
        except (TypeError, MonitorApiError) as ex:
            sout(str(ex) + '\n')

    def main(self):
        colorama.init()
        sout(f'Monitor connect to {self.host}:{self.port}\n')
        self.client = MonitorClient(self.host, self.port)
        num_tasks = len(self.client.get_task_list())
        sout(f'{num_tasks} tasks running, type help for commands!\n')
        while True:
            sout('Monitor> ')
            try:
                line = sys.stdin.readline()
                self.handler(line)
            except KeyboardInterrupt:
                break

    def command_ps(self, ident=None):
        if ident is None:
            return self._show_task_list()
        else:
            return self._show_task(int(ident))

    def _show_task_list(self):
        tasks = self.client.get_task_list()
        data = [('ID', 'Name', 'State', 'Waiting', 'Error', 'Result')]
        for task in tasks:
            error = task['error']
            if error:
                error = Fore.RED + error
            else:
                error = ''
            data.append((
                task['ident'],
                task['name'],
                task['state'],
                task['waiting'] or '',
                error,
                task['result'] or '',
            ))
        table = AsciiTable(data)
        sout(table.table)
        sout('\n')

    def _show_task(self, ident):
        task = self.client.get_task(ident)
        for k in ['ident', 'name', 'state', 'waiting', 'error']:
            sout(f'{k:>8}: {task[k]}\n')
        error_detail = (task['error_detail'] or '').strip()
        if error_detail:
            sout('-' * 60 + '\n')
            sout(error_detail + '\n')


def main():
    if len(sys.argv) != 2:
        print("Usage: python -m newio_kernel.monitor <HOST:PORT>")
        return
    host_port = sys.argv[1]
    if host_port in ['-h', '--help']:
        print("Usage: python -m newio_kernel.monitor <HOST:PORT>")
        return
    if host_port == '-':
        host, port = MONITOR_HOST, MONITOR_PORT
    else:
        host, port = host_port.split(':')
        port = int(port)
    shell = MonitorShell(host, port)
    shell.main()
