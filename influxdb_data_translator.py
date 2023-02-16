'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.

Do a customer modify for origin translator
'''
from collections import defaultdict
import logging

from yapic import json

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback
from cryptofeed.backends.http import HTTPCallback
from cryptofeed.defines import BID, ASK

LOG = logging.getLogger('feedhandler')


class CustomerInfluxCallback(HTTPCallback):
    def __init__(self, addr: str, org: str, bucket: str, token: str, key=None, **kwargs):
        """
        Parent class for InfluxDB callbacks

        influxDB schema
        ---------------
        MEASUREMENT | TAGS | FIELDS

        Measurement: Data Feed-Exchange (configurable)
        TAGS: symbol
        FIELDS: timestamp, amount, price, other funding specific fields

        Example data in InfluxDB
        ------------------------
        > select * from "book-COINBASE";
        name: COINBASE
        time                amount    symbol    price   side timestamp
        ----                ------    ----    -----   ---- ---------
        1542577584985404000 0.0018    BTC-USD 5536.17 bid  2018-11-18T21:46:24.963762Z
        1542577584985404000 0.0015    BTC-USD 5542    ask  2018-11-18T21:46:24.963762Z
        1542577585259616000 0.0018    BTC-USD 5536.17 bid  2018-11-18T21:46:25.256391Z

        Parameters
        ----------
        addr: str
          Address for connection. Should be in the format:
          http(s)://<ip addr>:port
        org: str
          Organization name for authentication
        bucket: str
          Bucket name for authentication
        token: str
          Token string for authentication
        key:
          key to use when writing data, will be a combination of key-datatype
        """
        super().__init__(addr, **kwargs)
        self.addr = f"{addr}/api/v2/write?org={org}&bucket={bucket}&precision=us"
        self.headers = {"Authorization": f"Token {token}"}

        self.session = None
        self.key = key if key else self.default_key
        self.numeric_type = float
        self.none_to = None
        self.running = True

    def format(self, data):
        ret = []
        for key, value in data.items():
            if key in {'timestamp', 'exchange', 'symbol', 'receipt_timestamp'}:
                continue
            if isinstance(value, str):
                ret.append(f'{key}="{value}"')
            else:
                ret.append(f'{key}={value}')
        return ','.join(ret)

    async def writer(self):
        while self.running:
            async with self.read_queue() as updates:
                for update in updates:
                    d = self.format(update)
                    timestamp = update["timestamp"]
                    timestamp_str = f',timestamp={timestamp}' if timestamp is not None else ''

                    if 'interval' in update:
                        print('not expect')
                        trades = f',trades={update["trades"]},' if update['trades'] else ','
                        update = f'{self.key}-{update["exchange"]},symbol={update["symbol"]},interval={update["interval"]} start={update["start"]},stop={update["stop"]}{trades}open={update["open"]},close={update["close"]},high={update["high"]},low={update["low"]},volume={update["volume"]}{timestamp_str},receipt_timestamp={update["receipt_timestamp"]} {int(update["receipt_timestamp"] * 1000000)}'
                    else:
                        update = f'{self.key}-{update["exchange"]},symbol={update["symbol"]} {d}{timestamp_str}'

                    await self.http_write(update, headers=self.headers)
        await self.session.close()


class TradeInfluxCallback(CustomerInfluxCallback, BackendCallback):
        default_key = 'trades'

        def format(self, data):
            return f'side="{data["side"]}",price={data["price"]},amount={data["amount"]}'


class BookInfluxCallback(CustomerInfluxCallback, BackendBookCallback):
    default_key = 'book'

    def __init__(self, *args, snapshots_only=True, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)

    def format(self, data):
        delta = 'delta' in data
        book = data['book'] if not delta else data['delta']
        bids_vol = []
        asks_vol = []
        out_str = ''
        for i, price in enumerate(book[BID]):
            out_str += f"bid{i + 1}={price},"
            bids_vol.append(book[BID][price])
        for i, price in enumerate(book[ASK]):
            out_str += f"ask{i + 1}={price},"
            asks_vol.append(book[ASK][price])
        assert len(bids_vol) == len(asks_vol)
        n = len(bids_vol)
        for i in range(n):
            out_str += f"bid_vol{i + 1}={bids_vol[i]},"
        for i in range(n - 1):
            out_str += f"ask_vol{i + 1}={asks_vol[i]},"
        out_str += f"ask_vol{n}={asks_vol[n - 1]}"

        if n != 10:
            print('should not happen')

        return out_str

