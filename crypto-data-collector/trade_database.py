from model import SQLiteDB


class DataSetBuilder:
    def __init__(self, pair):
        self.trade_db = SQLiteDB(pair)
        self.timeframes = []

    def add_timeframe(self, timeframe):
        self.timeframes.append(timeframe)

    def build_query(self):
        queries = []
        for timeframe in self.timeframes:
            trades_query = await self.trade_db.get_trades_by_timeframe(timeframe)
            tickers_query = await self.trade_db.get_tickers_by_timeframe(timeframe)
            buy_orders_query = await self.trade_db.get_orders_by_timeframe(timeframe, "buy")
            sell_orders_query = await self.trade_db.get_orders_by_timeframe(timeframe, "sell")
            queries.append((trades_query, tickers_query, buy_orders_query, sell_orders_query))
        return queries


if __name__ == "__main__":
    pair = "BTC_USD"
    query_builder = DataSetBuilder(pair)
    query_builder.add_timeframe(1)
    query_builder.add_timeframe(5)
    query_builder.add_timeframe(15)
    query_builder.add_timeframe(30)
    query_builder.add_timeframe(60)
    queries = query_builder.build_query()
