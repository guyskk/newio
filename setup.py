from setuptools import setup, find_packages


setup(
    name='newio',
    version='0.6.2',
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
    packages=find_packages('src'),
    package_dir={'': 'src'},
    python_requires='>=3.6',
    install_requires=['async-timeout>=3.0.0', 'aiomonitor>=0.3.1', 'aiodns>=1.1.1'],
    extras_require={
        'dev': [
            'invoke==1.0.0',
            'pytest==3.6.1',
            'pytest-cov==2.5.1',
            'coloredlogs==10.0',
            'twine==1.11.0',
            'pre-commit==1.10.2',
            'codecov==2.0.15',
            'bumpversion==0.5.3',
        ]
    },
)
