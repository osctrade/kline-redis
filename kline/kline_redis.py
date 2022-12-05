"""
取得所有 u 本位 K 线小时数据，并缓存在 redis 服务器中。
k 线数据保存在  redis 服务器中 f"kline_{interval}:{symbol}" 中，数据格式为列表，币安返回的数据，进行 pickle.dumps 后保存。保存的数据可能有重复，重复的取后面的记录。
f"kline_{interval}:{symbol}_cur" 中保存了当前 k 线数据
"""
import asyncio
import logging
import pickle
import random
from datetime import datetime
from typing import Union

import pandas as pd
from redis import asyncio as aioredis
from tenacity import retry, wait_exponential, stop_after_attempt
from trade_lib.message import set_dingding, dinding_send
import ccxt.pro

logger = logging.getLogger('kline')

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


# 程序实现
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


async def fetch_klines(binance,event: asyncio.Event, redis_url, interval, cached_time, debug=False):
    """
    主程序，使用 ws 取得 kline 数据
    """
    while True:
        redis_server = await aioredis.from_url(redis_url, decode_responses=False)
        queue = asyncio.Queue()
        tasks = []
        try:
            for i in range(30):
                task = asyncio.create_task(worker(queue, redis_server))
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

            # 订阅的频道 <symbol>@kline_<interval>
            steams = [f'{symbol.lower()}@kline_{interval}' for symbol in symbols]
            logger.info(steams)

            # or just clean it at begin
            rows = pd.Timedelta(cached_time) // pd.Timedelta(interval) + 1
            await clean_task(redis_server, symbols, interval, rows)
            # 如果 redis 数据不全，取 api 数据先补齐
            asyncio.create_task(
                check_redis_klines(redis_server, binance, interval, min(rows, 1000))
            )
            # use my handle_ohlcv
            coros = asyncio.as_completed([binance.watch_ohlcv(symbol,timeframe=interval) for symbol in symbols])
            for coro in coros:
                res = await coro

            ts = bm.futures_multiplex_socket(steams)
            #
            logger.info(f"redis kline started.")
            async with ts as tscm:
                while True:
                    res = await tscm.recv()
                    if ('data' in res) and (res['data']['e'] == 'kline'):
                        # 因为数据发送频率很快，所以这里一定要另开任务，否则肯定来不及处理！！
                        queue.put_nowait(res['data'])
                    if event.is_set():
                        # 当有事件时表明 订阅的 symbol 有变化，需要重新订阅
                        event.clear()
                        logger.info(f'当前交易 symbols 有变化，重新运行')
                        break
        except Exception as e:
            logger.exception(str(e))
            await dinding_send(f"kline_redis error:{e}")
            return
        finally:
            await queue.join()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await client.close_connection()
            await redis_server.close()
            logger.info(f"redis kline stop.")


async def check_symbol(binance: ccxt.pro.Exchange, event: asyncio.Event, redis_url):
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
    binance = ccxt.pro.binance(ccxt_config)
    try:
        await asyncio.wait({
            asyncio.create_task(check_symbol(binance, event, redis_url)),
            asyncio.create_task(fetch_klines(binance,event, redis_url, interval, cached_time, debug))
            # asyncio.create_task(redis_run_check(redis_url))
        }, return_when=asyncio.FIRST_COMPLETED)
    except Exception:
        logger.exception('kline_redis')
    finally:
        for task in asyncio.all_tasks():
            task.cancel()
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

    api_klines = await client.futures_klines(symbol=symbol, interval=interval, limit=rows)

    candle_begin_times = {kline['t'] for kline in redis_klines}

    add_klines = []
    for kline in api_klines:
        # api 取得的数据没有 symbol 字段
        kline = dict(zip(list(KLINE_COLUMNS.keys())[1:], kline))
        if kline['t'] not in candle_begin_times:
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
