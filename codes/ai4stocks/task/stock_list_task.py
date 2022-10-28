from ai4stocks.common.pendelum import DateTime, Duration
from ai4stocks.download.fast import StockListHandler
from ai4stocks.download.mysql import Operator
from ai4stocks.task import DownloadTask


class StockListTask(DownloadTask):
    def __init__(self,
                 operator: Operator,
                 start_time: DateTime = None):
        super().__init__(attr=StockListHandler(operator=operator).obtain_data,
                         start_time=start_time)

    def cycle(self) -> Duration:
        return Duration(days=15)

    def error_cycle(self) -> Duration:
        return Duration(minutes=5)
