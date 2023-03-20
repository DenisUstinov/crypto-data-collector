import time

import aiosqlite


class SQLiteDB:
    def __init__(self, pair: str):
        self.pair = pair
        self.db_name = f"{pair}.db"

    async def execute_query(self, query: str, data: tuple):
        async with aiosqlite.connect(self.db_name) as conn:
            async with conn.execute(query, data) as cur:
                rows = await cur.fetchall()
            await conn.commit()
            return rows

    async def create_trades_table(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS trades_{self.pair} (
                trade_id INTEGER,
                type TEXT,
                price REAL,
                quantity REAL,
                amount REAL,
                date INTEGER
            )
        """
        await self.execute_query(query, ())

    async def create_tickers_table(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS tickers_{self.pair} (
                buy_price REAL,
                sell_price REAL,
                last_trade REAL,
                high REAL,
                low REAL,
                avg REAL,
                vol REAL,
                vol_curr REAL,
                updated INTEGER
            )
        """
        await self.execute_query(query, ())

    async def create_orders_table(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS orders_{self.pair} (
                price REAL,
                quantity REAL,
                amount REAL,
                type REAL,
                date INTEGER
            )
        """
        await self.execute_query(query, ())

    async def insert_trade(self, trade_data):
        query = f"""
            INSERT INTO trades_{self.pair} (trade_id, type, price, quantity, amount, date)
            VALUES (:trade_id, :type, :price, :quantity, :amount, :date)
        """
        await self.execute_query(query, trade_data)

    async def insert_ticker(self, ticker_data):
        query = f"""
            INSERT INTO tickers_{self.pair} (buy_price, sell_price, last_trade, high, low, avg, vol, vol_curr, updated)
            VALUES (:buy_price, :sell_price, :last_trade, :high, :low, :avg, :vol, :vol_curr, :updated)
        """
        await self.execute_query(query, ticker_data)

    async def insert_order(self, order_data):
        query = f"""
            INSERT INTO orders_{self.pair} (price, quantity, amount, type, date)
            VALUES (:price, :quantity, :amount, :type, :date)
        """
        await self.execute_query(query, order_data)

    async def get_trades_by_timeframe(self, timeframe):
        end_time = int(time.time())
        start_time = end_time - (timeframe * 60)
        query = f"SELECT * FROM trades_{self.pair} WHERE date BETWEEN ? AND ?"
        rows = await self.execute_query(query, (start_time, end_time))
        return rows

    async def get_tickers_by_timeframe(self, timeframe):
        end_time = int(time.time())
        start_time = end_time - (timeframe * 60)
        query = f"SELECT * FROM tickers_{self.pair} WHERE updated BETWEEN ? AND ?"
        rows = await self.execute_query(query, (start_time, end_time))
        return rows

    async def get_orders_by_timeframe(self, timeframe, type_order):
        end_time = int(time.time())
        start_time = end_time - (timeframe * 60)
        query = f"SELECT * FROM orders_{self.pair} WHERE date BETWEEN ? AND ? AND type=?"
        rows = await self.execute_query(query, (start_time, end_time, type_order))
