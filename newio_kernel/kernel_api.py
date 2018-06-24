from traceback import format_exception


class KernelApiError(Exception):
    '''kernel api error'''


class KernelApi:
    def __init__(self, kernel):
        self.kernel = kernel

    def _format_task(self, task, with_stack=False):
        if task.is_alive and task.waiting is not None:
            waiting = repr(task.waiting)
        else:
            waiting = None
        error = error_type = result = None
        if not task.is_alive:
            if task.error is not None:
                error_type = type(task.error).__name__
                error = repr(task.error)
            else:
                result = repr(task.result)
        stack = None
        if with_stack:
            stack = self._get_task_stack(task)
        return dict(
            ident=task.ident,
            is_alive=task.is_alive,
            name=task.name,
            state=task.state,
            waiting=waiting,
            error=error,
            error_type=error_type,
            result=result,
            stack=stack,
        )

    def _get_task_stack(self, task):
        if task.is_alive:
            return task.format_stack()
        error_stack = format_exception(
            type(task.error), task.error, task.error.__traceback__)
        return ''.join(error_stack)

    async def get_task_list(self):
        ret = []
        for task in list(self.kernel.tasks):
            ret.append(self._format_task(task))
        return ret

    def _get_task_by_ident(self, ident):
        for t in list(self.kernel.tasks):
            if t.ident == ident:
                return t
        raise KernelApiError(f'task ident={ident} not found')

    async def get_task(self, ident):
        task = self._get_task_by_ident(ident)
        return self._format_task(task, with_stack=True)

    async def cancel_task(self, ident):
        task = self._get_task_by_ident(ident)
        if not task.is_alive:
            raise KernelApiError(f'task #{ident} not alive')
        self.kernel.engine.force_cancel(task)
        return self._format_task(task, with_stack=True)
