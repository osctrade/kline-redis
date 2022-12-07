import ccxt.pro


class binance(ccxt.pro.binance):
    """
    支持多 symbol 取得 k 线，发送到队列 queue
    """
    def __init__(self, config={}):
        super(binance, self).__init__(config)
        if 'queue' in config:
            self.queue = config['queue']
        else:
            raise ValueError('参数 queue 未设置！')

    async def watch_symbols_ohlcv(self, symbols, timeframe='1m', since=None, limit=None, params={}):
        """
        支持多 symbol
        :param str symbols: unified symbol of the market to fetch OHLCV data for
        :param str timeframe: the length of time each candle represents
        :param int|None since: timestamp in ms of the earliest candle to fetch
        :param int|None limit: the maximum amount of candles to fetch
        :param dict params: extra parameters specific to the binance api endpoint
        :returns [[int]]: A list of candles ordered as timestamp, open, high, low, close, volume
        """
        if len(symbols) > 200:
            raise ValueError(f"币安单个连接最多可以订阅 200 个")

        await self.load_markets()
        interval = self.timeframes[timeframe]
        messageHash = 'futures@kline' + '_' + interval
        options = self.safe_value(self.options, 'watchOHLCV', {})
        defaultType = self.safe_string(self.options, 'defaultType', 'spot')
        watchOHLCVType = self.safe_string_2(options, 'type', 'defaultType', defaultType)
        type = self.safe_string(params, 'type', watchOHLCVType)
        query = self.omit(params, 'type')
        url = self.urls['api']['ws'][type] + '/' + self.stream(type, messageHash)
        requestId = self.request_id(url)
        request = {
            'method': 'SUBSCRIBE',
            'params': [f'{symbol.lower()}@kline_{interval}' for symbol in symbols],
            'id': requestId,
        }
        subscribe = {
            'id': requestId,
        }
        return await self.watch(url, messageHash, self.extend(request, query), messageHash, subscribe)

    def handle_kline(self, client, message):
        kline = self.safe_value(message, 'k')
        interval = self.safe_string(kline, 'i')
        messageHash = 'futures@kline' + '_' + interval
        # print(message)
        self.queue.put_nowait(message)
        client.resolve(message, messageHash)

    def handle_message(self, client, message):
        event = self.safe_string(message, 'e')
        if event == 'kline':
            return self.handle_kline(client, message)
        else:
            return super(binance, self).handle_message(client, message)