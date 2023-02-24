from cryptofeed import FeedHandler
from cryptofeed.backends.backend import BackendCallback
from cryptofeed.defines import L2_BOOK, TRADES
from cryptofeed.backends.influxdb import BookInflux, TradeInflux, InfluxCallback
from cryptofeed.exchanges.binance import Binance
from cryptofeed.exchanges import Coinbase, OKX
from influxdb_data_translator import BookInfluxCallback, TradeInfluxCallback

INFLUX_ADDR = 'http://localhost:8086'
TOKEN = "DQE6Wdn3ZZGsOy6zF5_aMCzKNNIhzlIgmhLnvnp2DnEDIYhIugY7RcP2D7ztq69ZnH0EJh4ntM0-dUp8dSDk7w=="
ORG = "individual"
BUCKET = "cryptocurrency"
async def book(book, receipt_timestamp):
    print(f'Book received at {book.timestamp} for {book.exchange} - {book.symbol}, with {len(book.book)} entries. Top of book prices: {book.book.asks.index(0)[0]} - {book.book.bids.index(0)[0]}')
    if book.delta:
        print('delta')


def main():

    f = FeedHandler()
    f.add_feed(Binance(channels=[L2_BOOK], symbols=['BTC-USDT'], max_depth=10, depth_interval='100ms', callbacks={L2_BOOK: BookInfluxCallback(INFLUX_ADDR, ORG, BUCKET, TOKEN)}))
    # f.add_feed(Binance(channels=[TRADES], symbols=['BTC-USDT'], callbacks={TRADES: TradeInfluxCallback(INFLUX_ADDR, ORG, BUCKET, TOKEN)}))
    # f.add_feed(Binance(channels=[L2_BOOK], symbols=['BTC-USDT'], max_depth=10, depth_interval='100ms', callbacks={L2_BOOK: book}))
    # f.add_feed(Binance(channels=[TRADES], symbols=['BTC-USDT'], callbacks={TRADES: TradeInfluxCallback(INFLUX_ADDR, ORG, BUCKET, TOKEN)}))
    f.run()


if __name__ == '__main__':
    print('begin')
    main()