from pathlib import Path
from setuptools import find_packages as _find_packages

_version_file = Path(__file__).parent / 'version.txt'

__version__ = _version_file.read_text().strip()


def parse_requires(name):
    with open(name) as f:
        return f.read().splitlines()


package_data = {
    '_newio': ['version.txt']
}


def find_packages(target):
    target = target.replace('-', '_')
    packages = ['_newio', target]
    for name in _find_packages(target):
        packages.append(f'{target}.{name}')
    return packages
