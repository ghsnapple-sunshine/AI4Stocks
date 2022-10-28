from ai4stocks.common.pendelum import Date, DateTime, Duration
from ai4stocks.download import Para
from ai4stocks.download.mysql import Operator
from ai4stocks.download.slow import AkStockDailyHandler
from ai4stocks.task.download_task import DownloadTask


class StockDailyTask(DownloadTask):
    def __init__(self,
                 operator: Operator,
                 start_time: DateTime = None):
        super().__init__(attr=AkStockDailyHandler(operator=operator).obtain_data,
                         args=(Para().with_start_n_end(start=Date(2000, 1, 1), end=Date.today()),),
                         start_time=start_time)

    def cycle(self) -> Duration:
        return Duration(days=1)

    def error_cycle(self) -> Duration:
        return Duration(minutes=5)
