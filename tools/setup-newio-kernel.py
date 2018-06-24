import _newio
from setuptools import setup

version = _newio.__version__
requires = _newio.parse_requires('tools/requires-newio-kernel.txt')
requires.append(f'newio=={version}')

setup(
    name='newio-kernel',
    version=version,
    description='Newio Kernel',
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
        'newio_kernel',
    ],
    package_data={
        '_newio': ['version.txt']
    },
    python_requires='>=3.6',
    install_requires=requires,
)
