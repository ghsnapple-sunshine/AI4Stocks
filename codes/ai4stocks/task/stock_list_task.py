from pendulum import DateTime, Duration

from ai4stocks.download.fast import StockListHandler
from ai4stocks.download.mysql import RoleType, Operator
from ai4stocks.task import DownloadTask


class StockListTask(DownloadTask):
    def __init__(
            self,
            plan_time: DateTime = None
    ):
        super().__init__(
            obj=StockListHandler(operator=Operator(RoleType.DbStock)),
            method_name='download_and_save',
            plan_time=plan_time
        )

    def cycle(self) -> Duration:
        return Duration(days=15)

    def error_cycle(self) -> Duration:
        return Duration(minutes=5)
