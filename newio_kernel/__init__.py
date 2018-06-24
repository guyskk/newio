import _newio
from .kernel import Runner

run = Runner()

__version__ = _newio.__version__
__all__ = ('Runner', 'run',)
