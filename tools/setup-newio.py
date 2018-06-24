import _newio
from setuptools import setup

version = _newio.__version__
requires = _newio.parse_requires('tools/requires-newio.txt')

setup(
    name='newio',
    version=version,
    description='Newio',
    url='https://github.com/guyskk/newio',
    author='guyskk',
    author_email='guyskk@qq.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
    packages=[
        '_newio',
        'newio',
    ],
    package_data={
        '_newio': ['version.txt']
    },
    python_requires='>=3.6',
    install_requires=requires,
    extras_require={
        'kernel': [f'newio-kernel=={version}'],
    },
)
