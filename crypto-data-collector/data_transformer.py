from typing import List, Dict, Union, Optional
import statistics
import time


class DataTransformer:
    def __init__(self):
        pass

    def calc_mean(self, data: List[Dict[str, float]], field_name: str) -> float:
        values = [d[field_name] for d in data]
        return statistics.mean(values)

    def calc_median(self, data: List[Dict[str, float]], field_name: str) -> float:
        values = [d[field_name] for d in data]
        return statistics.median(values)

    def calc_sum(self, data: List[Dict[str, float]], field_name: str) -> float:
        values = [d[field_name] for d in data]
        return sum(values)

    def calc_trend(self, data: List[Dict[str, float]], field_name: str) -> bool:
        values = [d[field_name] for d in data]
        if len(values) < 2:
            return None
        return values[-1] > values[0]

    def get_data_by_timeframe(self, data: List[Dict[str, float]], timeframe: int) -> List[Dict[str, float]]:
        current_time = time.time()
        filtered_data = [d for d in data if current_time - d["date"] <= timeframe]
        return filtered_data

    def calc_mean_last_window(self, data: List[Dict[str, float]], field: str, timeframe: int, window_size: int) -> float:
        values = [row[field] for row in data]
        return sum(values) / len(values)

    def calculate_median(self, data: List[Dict], field: str) -> float:
        values = [row[field] for row in data]
        sorted_values = sorted(values)
        length = len(sorted_values)
        if length % 2 == 0:
            return (sorted_values[length//2] + sorted_values[length//2 - 1]) / 2
        else:
            return sorted_values[length//2]

    def calculate_sum(self, data: List[Dict], field: str) -> float:
        values = [row[field] for row in data]
        return sum(values)

    def calculate_trend(self, data: List[Dict], field: str) -> Optional[bool]:
        values = [row[field] for row in data]
        if len(values) < 2:
            return None
        if all(values[i] <= values[i+1] for i in range(len(values)-1)):
            return True
        elif all(values[i] >= values[i+1] for i in range(len(values)-1)):
            return False
        else:
            return None

    def process_data(self, data: List[Dict], field: str, window_size: int) -> Dict[str, Union[float, bool]]:
        result = {}
        values = [row[field] for row in data]
        if not values:
            return None
        values_sum = sum(values)
        result['sum'] = values_sum
        result['mean'] = values_sum / len(values)
        result['median'] = self.calculate_median(data, field)
        result['trend'] = self.calculate_trend(data, field)
        return result
