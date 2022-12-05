#!/usr/bin/env python
from setuptools import setup

install_requires = [
    "ccxt>=1.95.13",
    'tenacity',
    'pandas',
    'trade-lib',
    # 'PyYAML',
    # 'python-dotenv',
    # 'asyncclick',
    # 'pyotp',
    # 'plotly',
    'redis',
    # 'tabulate',
]

setup(
    name='kline-redis',
    description="K 线数据 redisk 缓存",
    version='0.1.0',
    py_modules=['kline'],
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'kline = kline.cli:main',
        ],
    },
)
