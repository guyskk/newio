import _newio
from .kernel import Runner
from .logger import init_logging  # noqa

run = Runner()

__version__ = _newio.__version__
__all__ = ('Runner', 'run')
