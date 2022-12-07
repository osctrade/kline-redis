"""
取得所有 u 本位 K 线小时数据，并缓存在 redis 服务器中。
k 线数据保存在  redis 服务器中 f"kline_{interval}:{symbol}" 中，数据格式为列表，币安返回的数据，进行 pickle.dumps 后保存。保存的数据可能有重复，重复的取后面的记录。
f"kline_{interval}:{symbol}_cur" 中保存了当前 k 线数据
"""
import asyncio
import logging
import pickle
from datetime import datetime

import pandas as pd
from redis import asyncio as aioredis
from tenacity import retry, wait_exponential, stop_after_attempt
from trade_lib.message import dinding_send

from kline_redis.api import get_symbols, kline_his_key, kline_cur_key, KLINE_COLUMNS, GRID_KLINE_STATUS, \
    GRID_PRICE_STATUS, \
    get_cur_candle_begin_time

logger = logging.getLogger('kline_redis')


# 程序运行前，补充没有的 k 线数据
@retry(reraise=True, wait=wait_exponential(multiplier=1, min=1, max=3), stop=stop_after_attempt(3))
async def repair_redis_kline(redis_server: aioredis.Redis, client, symbol, interval, rows=499):
    redis_key = kline_his_key(symbol, interval)
    max_rows = await redis_server.llen(redis_key)
    klines = await redis_server.lrange(redis_key, -max_rows, -1)

    kline = await redis_server.get(kline_cur_key(symbol, interval))
    if kline:
        klines.append(kline)
    redis_klines = [pickle.loads(kline) for kline in klines if kline]

    api_klines = await client.fapiPublic_get_klines({'symbol': symbol, 'interval': interval, 'limit': rows})
    candle_begin_times = {kline['t'] for kline in redis_klines}

    add_klines = []
    last = get_cur_candle_begin_time('15m', datetime.utcnow().timestamp(), 1)
    for kline in api_klines:
        # api 取得的数据没有 symbol 字段
        kline = dict(zip(list(KLINE_COLUMNS.keys())[1:], kline))
        if kline['t'] not in candle_begin_times and (pd.to_datetime(kline['t'], unit='ms') <= last):
            kline['s'] = symbol
            add_klines.append(kline)

    if add_klines:
        klines = [pickle.dumps(kline) for kline in add_klines]
        ret = await redis_server.rpush(redis_key, *klines)
        logger.info(f"redis 修正 {symbol}: total: {ret}, added: {len(klines)}")


async def check_redis_klines(redis_server: aioredis.Redis, client, interval, rows=499):
    """
    对 redis 中的数据进行检查，如果没有的话，补齐最近的 k 线数据，rows 最大设为 1000, 对15分的数据来说，就是大约10天内如果 k 线数据有问题，都能通过 api 补齐。
    """
    # 等程序开始运行后再查看有没有需要补齐的数据
    try:
        await asyncio.sleep(25)
        symbols = await get_symbols(redis_server)
        logger.info('check redis kline ...')

        for symbol in symbols:
            await repair_redis_kline(redis_server, client, symbol, interval, rows)

        logger.info('check redis kline 完成！')

        # 以下方式会出现 redis 60 错误，暂时没办法解决
        # tasks = [redis_kline_and_api_kline(redis_server, client, symbol, interval, rows)
        #          for symbol in symbols]
        # await asyncio.gather(*tasks)
    except Exception:
        logger.exception('check_redis_klines')


async def is_redis_ok(redis_server):
    kline_status = await redis_server.get(GRID_KLINE_STATUS)
    if not kline_status:
        logger.error(f"redis kline 检测失败, kline_status None")
    price_status = await redis_server.get(GRID_PRICE_STATUS)
    if not price_status:
        logger.error(f"redis kline 检测失败, kline_price None")

    return bool(kline_status and price_status) and (await redis_check_last_time(redis_server))


async def redis_check_last_time(redis_server):
    last = get_cur_candle_begin_time('15m', datetime.utcnow().timestamp(), 1)
    # get btc last kline time
    redis_key = kline_his_key("BTCUSDT", '15m')
    klines = await redis_server.lrange(redis_key, -1, -1)
    kline = pickle.loads(klines[0])
    candle_begin_time = pd.to_datetime(kline['t'], unit='ms')
    if candle_begin_time == last:
        return True
    else:
        logger.error(f"redis kline 检测失败，cur kline time: {last}, but redis get {candle_begin_time}")
        return False


async def redis_run_check(redis_url):
    """
    每 1 分钟检测 k 线数据是否完整
    """
    await asyncio.sleep(5 * 60)
    redis_server = await aioredis.from_url(redis_url, decode_responses=False)
    try:
        await redis_server.set(GRID_KLINE_STATUS, 'ok')
        await redis_server.set(GRID_PRICE_STATUS, 'ok')
        while True:
            await asyncio.sleep(60)
            if not await is_redis_ok(redis_server):
                msg = 'k 线数据状态有问题，请检查！'
                logger.error(msg)
                await dinding_send(msg)
                return False
            else:
                logger.debug("get kline status: ok")
    finally:
        if redis_server:
            await redis_server.close()
