from influxdb_client import InfluxDBClient, Point, WriteOptions



INFLUX_ADDR = ''
TOKEN = ""
ORG = ""
BUCKET = ""

client = InfluxDBClient(url=INFLUX_ADDR, token=TOKEN, org=ORG)

book_query= '''
from(bucket: "btc-usdt")
  |> range(start: -15m, stop: now())
  |> filter(fn: (r) => r["_measurement"] == "book-OKX")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  '''

order_books = client.query_api().query_data_frame(org=ORG, query=book_query)
order_books.drop(columns=['result', 'table','_start','_stop', '_time', "_measurement"])
# print(order_books.head(1))
order_books = order_books[["timestamp", "symbol"
  , "bid1", "bid2", "bid3", "bid4", "bid5", "bid6", "bid7", "bid8", "bid9", "bid10"
  , "ask1", "ask2", "ask3", "ask4", "ask5", "ask6", "ask7", "ask8", "ask9", "ask10"
  , "bid_vol1", "bid_vol2", "bid_vol3", "bid_vol4", "bid_vol5", "bid_vol6", "bid_vol7", "bid_vol8", "bid_vol9", "bid_vol10"
  , "ask_vol1", "ask_vol2", "ask_vol3", "ask_vol4", "ask_vol5", "ask_vol6", "ask_vol7", "ask_vol8", "ask_vol9", "ask_vol10"]]
print(order_books.head(2))

trade_query= '''
from(bucket: "btc-usdt")
  |> range(start: -1h, stop: now())
  |> filter(fn: (r) => r["_measurement"] == "trades-COINBASE")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  '''
trade_info = client.query_api().query_data_frame(org=ORG, query=trade_query)
trade_info.drop(columns=['result', 'table','_start','_stop', '_time', "_measurement"])
print(trade_info.head(2))

client.__del__()
