import asyncio
import logging
import os
import signal
import sys

from trade_lib.util import read_config

import kline_redis.kline_redis
from kline_redis.util import set_logging_config

logger = logging.getLogger('kline_redis')


async def start(config):
    try:
        logger.info(f'start kline redis....')
        await kline_redis.kline_redis.main(config)
    except asyncio.CancelledError:
        [task.cancel() for task in asyncio.all_tasks()]
        await asyncio.gather(*asyncio.all_tasks())


def handler_stop_signals(signum, frame):
    logger.info(f"收到退出信号，程序退出!")
    [task.cancel() for task in asyncio.all_tasks()]


def main():
    logger.debug('redis k 线缓存')
    signal.signal(signal.SIGINT, handler_stop_signals)
    signal.signal(signal.SIGTERM, handler_stop_signals)

    set_logging_config()

    config_file = sys.argv[1] if len(sys.argv) == 2 else None or os.getenv('KLINE_CONFIG')
    if not config_file:
        logger.error(f'请指定运行参数文件，或设置 KLINE_CONFIG 环境变量！')
        return 1
    config_file = os.path.expanduser(config_file)
    config = read_config(config_file)
    try:
        asyncio.run(start(config))
    except KeyboardInterrupt:
        logging.info('Ctrl+C 完成退出')
    except Exception as e:
        logging.exception(e)


if __name__ == '__main__':
    main()
