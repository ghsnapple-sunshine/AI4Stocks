from pendulum import DateTime, Duration

from ai4stocks.download.akshare.stock_daily_handler import StockDailyHandler
from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.task.download_task import DownloadTask


class StockDailyTask(DownloadTask):
    def __init__(self,
                 plan_time: DateTime = None):
        super().__init__(
            obj=StockDailyHandler(op=MysqlOperator(MysqlRole.DbStock)),
            method_name='DownloadAndSave',
            kwargs={
                'start_time': DateTime(year=2020, month=1, day=1),
                'end_time': DateTime.now()
            },
            plan_time=plan_time
        )

    def Cycle(self) -> Duration:
        return Duration(days=1)

    def ErrorCycle(self) -> Duration:
        return Duration(minutes=5)
