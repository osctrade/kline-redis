"""
取得所有 u 本位 K 线小时数据，并缓存在 redis 服务器中。
k 线数据保存在  redis 服务器中 f"kline_{interval}:{symbol}" 中，数据格式为列表，币安返回的数据，进行 pickle.dumps 后保存。保存的数据可能有重复，重复的取后面的记录。
f"kline_{interval}:{symbol}_cur" 中保存了当前 k 线数据
"""
import asyncio
import logging
import pickle
import random

import pandas as pd
from redis import asyncio as aioredis
from trade_lib.message import dinding_send

from kline_redis import binance_kline
from kline_redis.api import kline_his_key, GRID_KLINE_STATUS, kline_cur_key, GRID_PRICE_STATUS, EXCHANGE_INFO_KEY, \
    get_symbols, get_kline, kline_check, get_current_price
from kline_redis.repair import check_redis_klines, redis_run_check

logger = logging.getLogger('kline_redis')


async def update_kline(redis_server: aioredis.Redis, res):
    """
    更新 redis 中的 k 线数据
    币安返回数据 res 格式见： https://binance-docs.github.io/apidocs/futures/cn/#k-6
    """
    try:
        symbol = res['s']
        interval = res['k']['i']

        if res['k']['x']:
            # k 线已完结
            redis_key = kline_his_key(symbol, interval)
            await redis_server.rpush(redis_key, pickle.dumps(res['k']))
            if symbol == 'BTCUSDT':
                # 设置 18 分钟后失效，对于 15 分钟的 k 线数据应该够用
                await redis_server.expire(GRID_KLINE_STATUS, 16 * 60)
                candle_begin_time = pd.to_datetime(res['k']['t'], unit='ms') + pd.Timedelta(hours=8)
                logger.debug(f"kline updated {interval}: {symbol} {candle_begin_time}")
        else:
            redis_key = kline_cur_key(symbol, interval)
            await redis_server.set(redis_key, pickle.dumps(res['k']))
            if symbol == 'BTCUSDT':
                await redis_server.expire(GRID_PRICE_STATUS, 5)
                # candle_begin_time = pd.to_datetime(res['k']['t'], unit='ms')
                # logger.debug(f"kline price updated {interval}: {symbol} {candle_begin_time}")
    except Exception as e:
        logger.exception('update kline:')


async def _clean_symbol(redis_server, key, max_rows):
    await redis_server.ltrim(key, -max_rows, -1)


async def clean_task(redis_server, symbols, interval, rows):
    """
    清除过期 k 线数据
    interval: 1h,5m
    """

    task = [_clean_symbol(redis_server, kline_his_key(symbol, interval), rows) for symbol in symbols]

    await asyncio.gather(*task)


async def worker(queue, redis_server):
    while True:
        try:
            res = await queue.get()
            await update_kline(redis_server, res)
        except Exception:
            logger.exception('kline worker:')
        finally:
            queue.task_done()


async def fetch_klines(binance: binance_kline.binance, event: asyncio.Event, redis_url, interval, cached_time,
                       debug=False):
    """
    主程序，使用 ws 取得 kline 数据
    """

    redis_server = await aioredis.from_url(redis_url, decode_responses=False)
    tasks = []
    try:
        for i in range(30):
            task = asyncio.create_task(worker(binance.queue, redis_server))
            tasks.append(task)

        exchange_info = await binance.fapiPublic_get_exchangeinfo()
        # save current exchange_info
        await redis_server.set(EXCHANGE_INFO_KEY, pickle.dumps(exchange_info))

        symbols = [symbol['symbol'] for symbol in exchange_info['symbols']
                   if (symbol['status'] == 'TRADING') and (symbol['contractType'] == 'PERPETUAL')]
        if debug:
            symbols = symbols[:1]

        # set redis symbols
        await redis_server.delete('kline_symbols')
        await redis_server.sadd('kline_symbols', *symbols)

        # or just clean it at begin
        rows = pd.Timedelta(cached_time) // pd.Timedelta(interval) + 1
        await clean_task(redis_server, symbols, interval, rows)
        # 如果 redis 数据不全，取 api 数据先补齐
        asyncio.create_task(
            check_redis_klines(redis_server, binance, interval, min(rows, 1000))
        )
        # use my handle_ohlcv
        await binance.watch_symbols_ohlcv(symbols, timeframe=interval)
        logger.info(f"redis kline started.")
        while True:
            if event.is_set():
                # 当有事件时表明 订阅的 symbol 有变化，需要重新订阅
                event.clear()
                logger.info(f'当前交易 symbols 有变化，重新运行')
                break
            await asyncio.sleep(60)

    except Exception as e:
        logger.exception(str(e))
        await dinding_send(f"kline_redis error:{e}")
        return
    finally:
        await binance.queue.join()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await redis_server.close()
        logger.info(f"fetch klines stop.")


async def check_symbol(binance: binance_kline.binance, event: asyncio.Event, redis_url):
    """
    检测交易所交易的永续合约 symbol 是否发生了变化，如发生变化，设置 event
    """
    redis_server = await aioredis.from_url(redis_url, decode_responses=False)
    try:
        while True:
            await asyncio.sleep(random.randint(5, 30))
            await asyncio.sleep(60 * 60)  # 60m 检测一次
            exchange_info = await binance.fapiPublic_get_exchangeinfo()
            await redis_server.set(EXCHANGE_INFO_KEY, pickle.dumps(exchange_info))
            symbols = {symbol['symbol'] for symbol in exchange_info['symbols']
                       if (symbol['status'] == 'TRADING') and (symbol['contractType'] == 'PERPETUAL')}

            redis_symbols = await get_symbols(redis_server)
            if redis_symbols != symbols:
                logger.info('交易所交易对发生变化，正通知程序重取！')
                event.set()

    except Exception as e:
        logger.exception(e)
        await dinding_send(e)
    finally:
        await redis_server.close()


async def run(ccxt_config, redis_url, interval, cached_time, debug):
    event = asyncio.Event()
    queue = asyncio.Queue()
    ccxt_config.update({'queue': queue,
                        'options': {
                            'defaultType': 'future',  # spot, margin, future, delivery
                        }})
    binance = binance_kline.binance(ccxt_config)
    try:
        await asyncio.wait({
            asyncio.create_task(check_symbol(binance, event, redis_url)),
            asyncio.create_task(fetch_klines(binance, event, redis_url, interval, cached_time, debug)),
            asyncio.create_task(redis_run_check(redis_url))
        }, return_when=asyncio.FIRST_COMPLETED)
    except Exception:
        logger.exception('kline_redis')
    finally:
        await binance.close()


# 插件入口
async def main(configs):
    try:
        interval = configs.get('interval', '15m')
        debug = configs.get('debug', False)
        cached_time = configs.get('cached_times', '30days')
        redis_url = configs.get('redis_url')
        ccxt_config = configs.get('ccxt', {})
        await run(ccxt_config, redis_url, interval, cached_time, debug)
    except Exception as e:
        logger.exception(str(e))
        await dinding_send(f"kline_redis error:{e}")
    finally:
        [task.cancel() for task in asyncio.all_tasks()]
        logger.info(f"redis kline stop.")


async def get_kline_main(redis_url, symbol, interval, nums=10):
    redis_server = await aioredis.from_url(redis_url, decode_responses=False)
    # symbols = await get_symbols(redis_server)
    # print(symbols)

    klines = await get_kline(redis_server, symbol, interval, nums)
    print(klines)
    ret = kline_check(klines, interval, True)
    print('check kline:', ret)
    price = await get_current_price(redis_server, 'BTCUSDT')
    print(price)
    await redis_server.close()
