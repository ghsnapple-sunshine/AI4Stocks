from pendulum import DateTime, Duration

from buffett.download.mysql import Operator
from buffett.download.slow import ReformHandler
from buffett.task.download_task import DownloadTask


class StockReformTask(DownloadTask):
    def __init__(self,
                 operator: Operator,
                 start_time: DateTime = None):
        super().__init__(attr=ReformHandler(operator=operator).reform_data,
                         start_time=start_time)

    def cycle(self) -> Duration:
        return Duration(days=1)

    def error_cycle(self) -> Duration:
        return Duration(minutes=5)
