class TaskCanceled(BaseException):
    '''
    Exception raised when task cancelled.

    It's used to unwind coroutine stack and cleanup resources,
    the exception **MUST NOT** be catch without reraise.

    The exception directly inherits from BaseException instead of Exception
    so as to not be accidentally caught by code that catches Exception.
    '''


class NewioError(Exception):
    '''Base class for Newio-related exceptions.'''


class TaskTimeout(NewioError):
    '''Exception raised when task timeout.'''
