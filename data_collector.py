from cryptofeed import FeedHandler
from cryptofeed.backends.backend import BackendCallback
from cryptofeed.defines import L2_BOOK, TRADES
from cryptofeed.backends.influxdb import BookInflux, TradeInflux, InfluxCallback
from cryptofeed.exchanges.binance import Binance
from influxdb_data_translator import BookInfluxCallback, TradeInfluxCallback

INFLUX_ADDR = 'http://localhost:8086'
TOKEN = "UdsE0MwUPCxYUfCGzNcAY19OHVa5WnclHgrbnIUoMYUkPvWl0ohuaP7I_LPQGvyKIkmY1sZ14la-m74avY7xaw=="
ORG = "individual"
BUCKET = "cryptocurrency"

def main():

    f = FeedHandler()
    f.add_feed(Binance(channels=[L2_BOOK], symbols=['BTC-USDT'], max_depth=10, depth_interval='100ms', callbacks={L2_BOOK: BookInfluxCallback(INFLUX_ADDR, ORG, BUCKET, TOKEN)}))
    f.add_feed(Binance(channels=[TRADES], symbols=['BTC-USDT'], callbacks={TRADES: TradeInfluxCallback(INFLUX_ADDR, ORG, BUCKET, TOKEN)}))
    f.run()


if __name__ == '__main__':
    print('begin')
    main()