from buffett.common.pendelum import DateTime, Duration
from buffett.download.fast import StockListHandler
from buffett.download.mysql import Operator
from buffett.task.download_task import DownloadTask


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
