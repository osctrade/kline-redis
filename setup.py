#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = [
    "ccxt>=1.95.13",
    'tenacity',
    'pandas',
    'trade-lib',
    'PyYAML',
    'redis',
]

setup(
    name='kline-redis',
    description="K 线数据 redisk 缓存",
    version='0.1.1',
    py_modules=['kline'],
    packages=find_packages(exclude=["tests"]),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'kline = kline.cli:main',
        ],
    },
)
