import asyncio
from exmo.websocket import Client
from model import SQLiteDB


async def process_api_response(data: dict):
    event = data.get('event')
    if event == 'update':
        topic = data.get('topic')
        if topic.startswith('spot/trades:'):
            await db.create_trades_table()
            await db.insert_trade(data['data'])
        elif topic.startswith('spot/ticker:'):
            await db.create_ticker_table()
            await db.insert_ticker(data['data'])
        else:
            raise ValueError(f"Unrecognized data received: {topic}")


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

    client = Client(process_api_response)
    task = [
        asyncio.create_task(client.listen(trades)),
        asyncio.create_task(client.listen(ticker))
    ]
    await asyncio.gather(*task)


if __name__ == "__main__":
    pair = "BTC_USD"
    db = SQLiteDB(pair)
    asyncio.run(main(pair))
