from model import SQLiteDB
from data_transformer import DataTransformer
from typing import List, Dict, Any, Callable


class DataSetBuilder:
    def __init__(self, pair):
        self.db = SQLiteDB(pair)
        self.dt = DataTransformer()
        self.timeframes = []

    def add_timeframe(self, timeframe):
        self.timeframes.append(timeframe)

    def build_query(self):
        queries = []
        for timeframe in self.timeframes:
            trades_query = await self.db.get_trades_by_timeframe(timeframe)
            tickers_query = await self.db.get_tickers_by_timeframe(timeframe)
            buy_orders_query = await self.db.get_orders_by_timeframe(timeframe, "buy")
            sell_orders_query = await self.db.get_orders_by_timeframe(timeframe, "sell")
            queries.append((trades_query, tickers_query, buy_orders_query, sell_orders_query))
        return queries

    def group_data_by_timestamp(data: List[Dict[str, Any]], timestamp_field: str,
                                handler: Callable[[List[Dict[str, Any]]], None]) -> None:
        current_time = data[0][timestamp_field]
        group = []
        for i, item in enumerate(data):
            group.append(item)
            if i == len(data) - 1 or item[timestamp_field] - current_time >= 60:
                handler(group)
                current_time += 60


if __name__ == "__main__":
    pair = "BTC_USD"
    query_builder = DataSetBuilder(pair)
    query_builder.add_timeframe(60)
    queries = query_builder.build_query()
