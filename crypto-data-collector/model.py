import aiosqlite


class SQLiteDB:
    def __init__(self, pair: str):
        self.pair = pair
        self.db_name = f"{pair}.db"

    async def create_trades_table(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS trades_{self.pair} (
                trade_id INTEGER PRIMARY KEY,
                type TEXT,
                price REAL,
                quantity REAL,
                amount REAL,
                date INTEGER
            )
        """
        async with aiosqlite.connect(self.db_name) as conn:
            await conn.execute(query)
            await conn.commit()

    async def create_ticker_table(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS ticker_{self.pair} (
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
        async with aiosqlite.connect(self.db_name) as conn:
            await conn.execute(query)
            await conn.commit()

    async def insert_trade(self, trade_data):
        query = f"""
            INSERT INTO trades_{self.pair} (trade_id, type, price, quantity, amount, date)
            VALUES (:trade_id, :type, :price, :quantity, :amount, :date)
        """
        async with aiosqlite.connect(self.db_name) as conn:
            await conn.execute(query, trade_data)
            await conn.commit()

    async def insert_ticker(self, ticker_data):
        query = f"""
            INSERT INTO ticker_{self.pair} (buy_price, sell_price, last_trade, high, low, avg, vol, vol_curr, updated)
            VALUES (:buy_price, :sell_price, :last_trade, :high, :low, :avg, :vol, :vol_curr, :updated)
        """
        async with aiosqlite.connect(self.db_name) as conn:
            await conn.execute(query, ticker_data)
            await conn.commit()
