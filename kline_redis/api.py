import logging
import pickle
from datetime import datetime
from typing import Union

import pandas as pd
from redis import asyncio as aioredis
from tenacity import retry, wait_exponential, stop_after_attempt

logger = logging.getLogger('kline_redis')

pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)

GRID_KLINE_STATUS = 'kline_status'
GRID_PRICE_STATUS = 'price_status'

KLINE_COLUMNS = {
    's': 'symbol',
    't': 'candle_begin_time',
    'o': 'open',
    'h': 'high',
    'l': 'low',
    'c': 'close',
    'v': 'volume',
    'T': 'close_time',
    'q': 'quote_volume',
    'n': 'trade_num',
    'V': 'taker_buy_base_asset_volume',
    'Q': 'taker_buy_quote_asset_volume',
}

# redis keys
# type list ,  k 线数据
KLINE_HIS_KEY = 'kline_{interval}:{symbol}'
# type string , 当前最近的 k 线数据
KLINE_CUR_KEY = 'kline_{interval}:{symbol}:cur'
# type set, 当前 symbols
KLINE_CUR_SYMBOLS = 'kline_symbols'
# save current exchange_info
EXCHANGE_INFO_KEY = 'EXCHANGE_INFO'


# some helper function
def kline_his_key(symbol, interval):
    return KLINE_HIS_KEY.format(interval=interval, symbol=symbol)


def kline_cur_key(symbol, interval):
    return KLINE_CUR_KEY.format(interval=interval, symbol=symbol)


async def get_redis(redis_url):
    return await aioredis.from_url(redis_url, decode_responses=False)


def get_cur_candle_begin_time(interval, ts=datetime.utcnow().timestamp(), sub=0):
    """
    取当前最近的 k 线开始时间
    sub 为 0，或 1
    """
    # print(ts)
    interval_seconds = pd.Timedelta(interval).total_seconds()
    last = (ts // interval_seconds - sub) * interval_seconds
    last = datetime.fromtimestamp(last)
    return last


async def get_exchange_info(redis_server: aioredis.Redis):
    """
    取当前币安交易所 exchange_info
    """
    info = await redis_server.get(EXCHANGE_INFO_KEY)
    return pickle.loads(info) if info else None


# 供客户端调用 api
@retry(reraise=True, wait=wait_exponential(multiplier=1, min=1, max=3), stop=stop_after_attempt(3))
async def get_symbols(redis_server: aioredis.Redis):
    """
    返回当前正在记录 k 线的所有 symbol
    """
    symbols = await redis_server.smembers('kline_symbols')
    return {symbol.decode() for symbol in symbols}


async def get_current_price(redis_server: aioredis.Redis, symbol) -> float:
    """
    取最近的价格
    """
    redis_key = kline_cur_key(symbol, '15m')
    res = await redis_server.get(redis_key)
    res = pickle.loads(res)
    return float(res['c'])


async def get_kline(redis_server: aioredis.Redis, symbol, interval, limit=1000, check=True) -> Union[
    pd.DataFrame, None]:
    """
    用于客户端取当前最新的 last_nums 根 k 线数据，
    @param redis_server: await aioredis.from_url(redis_url, decode_responses=False), 注意 decode_responses=False
    @param symbol:  symbol
    @param interval: such as 1m , 1h，由 redis 的 15m k 线组合而成
    @param limit: 取最后的多少根 k 线数据
    @param check: 是否检测 k 线数据
    @return DateFrame or None
    """
    try:
        limit = pd.Timedelta(interval) // pd.Timedelta('15m') * limit
        df: pd.DataFrame = await _get_kline(redis_server, symbol, '15m', limit)

        if df is None:
            return None

        if check:
            if not kline_check(df, '15m', False):
                return None

        if interval == '15m':
            return df

        df.set_index('candle_begin_time', inplace=True)
        df = df.resample(rule=interval).agg({
            'symbol': 'last',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'last',
            'quote_volume': 'last',
            'trade_num': 'last',
            'taker_buy_base_asset_volume': 'last',
            'taker_buy_quote_asset_volume': 'last',
        })
        df.reset_index(inplace=True)
        return df
    except Exception as e:
        logger.exception('取 k 线数据出错')
        return None


@retry(reraise=True, wait=wait_exponential(multiplier=1, min=1, max=3), stop=stop_after_attempt(3))
async def _get_kline(redis_server: aioredis.Redis, symbol, interval, last_nums=1000):
    """
    用于客户端取当前最新的 last_nums 根 k 线数据，
    @param redis_server: await aioredis.from_url(redis_url, decode_responses=False), 注意 decode_responses=False
    @param symbol:  symbol
    @param interval: such as 1m , 1h , 需要 redis 支持
    @param last_nums: 取最后的多少根 k 线数据
    @param check: 是否检测 k 线数据
    @return DateFrame or None
    """
    try:
        redis_key = kline_his_key(symbol, interval)
        klines = await redis_server.lrange(redis_key, -last_nums, -1)

        # 取最新 k 线数据
        redis_key = kline_cur_key(symbol, interval)
        kline = await redis_server.get(redis_key)
        if kline:
            klines.append(kline)

        df = pd.DataFrame([pickle.loads(kline) for kline in klines if kline])
        if df.empty:
            return None
        df.rename(columns=KLINE_COLUMNS, inplace=True)
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')

        df.sort_values(by=['candle_begin_time'], inplace=True)
        df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
        columns = ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 'taker_buy_base_asset_volume',
                   'taker_buy_quote_asset_volume']
        for column in columns:
            df[column] = pd.to_numeric(df[column])

        df.reset_index(drop=True, inplace=True)
        return df[KLINE_COLUMNS.values()]

    except Exception as e:
        logger.exception('取 k 线数据出错')
        return None


def kline_check(df, interval, last_time_check=False):
    """
    对返回的 k 线数据进行检查
    """
    # 检测数据是否连续
    rows = df.shape[0]
    dt = df['candle_begin_time'][rows - 1] - df['candle_begin_time'][0]
    if not dt / pd.Timedelta(interval) == (rows - 1):
        logger.debug('时间跟数量不一致！')
        return False

    # 最后一根 k 线是否是最新的
    if last_time_check:
        last = get_cur_candle_begin_time(interval, datetime.utcnow().timestamp())
        if df['candle_begin_time'][rows - 1] == last:
            return True
        else:
            logger.debug(f"最后时间不一致！ kline: {df['candle_begin_time'][rows - 1]}, last:{last}")
            return False
    else:
        return True
