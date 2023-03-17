import asyncio
from exmo.websocket import Client
from model import SQLiteDB


async def process_api_response(data: dict):
    event = data.get('event')
    if event == 'update' or event == 'snapshot':
        topic = data.get('topic')
        if topic.startswith('spot/trades:'):
            await db.create_trades_table()
            await db.insert_trade(data['data'])
        elif topic.startswith('spot/ticker:'):
            await db.create_tickers_table()
            await db.insert_ticker(data['data'])
        elif topic.startswith('spot/order_book_snapshots:'):
            await db.create_orders_table()
            ask_data = {
                'price': data['data']['ask'][0][0],
                'quantity': data['data']['ask'][0][1],
                'amount': data['data']['ask'][0][2],
                'type': 'ask',
                'date': data['ts']
            }
            print(ask_data)
            await db.insert_order(ask_data)
            bid_data = {
                'price': data['data']['bid'][0][0],
                'quantity': data['data']['bid'][0][1],
                'amount': data['data']['bid'][0][2],
                'type': 'bid',
                'date': data['ts']
            }
            print(bid_data)
            await db.insert_order(bid_data)


async def main(pair):
    trades = {
            "url": "wss://ws-api.exmo.com:443/v1/public",
            "init_messages": (
                f'{{"id":1,"method":"subscribe","topics":["spot/trades:{pair}"]}}',
            )
        }
    ticker = {
            "url": "wss://ws-api.exmo.com:443/v1/public",
            "init_messages": (
                f'{{"id":1,"method":"subscribe","topics":["spot/ticker:{pair}"]}}',
            )
        }
    orders = {
        "url": "wss://ws-api.exmo.com:443/v1/public",
        "init_messages": (
            f'{{"id":1,"method":"subscribe","topics":["spot/order_book_snapshots:{pair}"]}}',
        )
    }

    client = Client(process_api_response)
    task = [
        asyncio.create_task(client.listen(trades)),
        asyncio.create_task(client.listen(ticker)),
        asyncio.create_task(client.listen(orders))
    ]
    await asyncio.gather(*task)


if __name__ == "__main__":
    currency_pair = "BTC_USD"
    db = SQLiteDB(currency_pair)
    asyncio.run(main(currency_pair))
