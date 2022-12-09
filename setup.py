#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = [
    "ccxt>=2.2.79",
    'tenacity',
    'pandas',
    'trade-lib',
    'PyYAML',
    'redis',
]

setup(
    name='kline-redis',
    description="K 线数据 redisk 缓存",
    version='0.1.3',
    py_modules=['kline_redis'],
    packages=find_packages(exclude=["tests"]),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'kline-redis = kline_redis.cli:main',
        ],
    },
)
