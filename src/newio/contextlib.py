# flake8: noqa
"""从Python3.7移植过来的contextlib"""
try:
    from contextlib import AsyncExitStack
    from contextlib import *
except ImportError:
    from .compat.contextlib import *
