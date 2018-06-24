import os.path
import linecache
import traceback


# Internal functions used for debugging/diagnostics
def _get_stack(coro):
    '''
    Extracts a list of stack frames from a chain of generator/coroutine calls
    '''
    frames = []
    while coro:
        if hasattr(coro, 'cr_frame'):
            f = coro.cr_frame
            coro = coro.cr_await
        elif hasattr(coro, 'ag_frame'):
            f = coro.ag_frame
            coro = coro.ag_await
        elif hasattr(coro, 'gi_frame'):
            f = coro.gi_frame
            coro = coro.gi_yieldfrom
        else:
            # Note: Can't proceed further.  Need the ags_gen or agt_gen attribute
            # from an asynchronous generator.  See https://bugs.python.org/issue32810
            f = None
            coro = None
        if f is not None:
            frames.append(f)
    return frames


# Create a stack traceback for a task
def format_task_stack(task, complete=False):
    '''
    Formats a traceback from a stack of coroutines/generators
    '''
    dirname = os.path.dirname(__file__)
    extracted_list = []
    checked = set()
    for f in _get_stack(task.coro):
        lineno = f.f_lineno
        co = f.f_code
        filename = co.co_filename
        name = co.co_name
        if not complete and os.path.dirname(filename) == dirname:
            continue
        if filename not in checked:
            checked.add(filename)
            linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        extracted_list.append((filename, lineno, name, line))
    if not extracted_list:
        resp = 'No stack for %r' % task
    else:
        resp = 'Stack for %r (most recent call last):\n' % task
        resp += ''.join(traceback.format_list(extracted_list))
    return resp
