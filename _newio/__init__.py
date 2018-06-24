from pathlib import Path

_version_file = Path(__file__).parent / 'version.txt'

__version__ = _version_file.read_text().strip()


def parse_requires(name):
    with open(name) as f:
        return f.read().splitlines()
