from pendulum import DateTime, Duration

from ai4stocks.download.akshare.stock_daily_handler import StockDailyHandler
from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.task.download_task import DownloadTask


class StockDailyTask(DownloadTask):
    def __init__(self,
                 plan_time: DateTime = None):
        self.obj = StockDailyHandler(
            op=MysqlOperator(MysqlRole.DbStock)
        )
        self.method_name = 'DownloadAndSave'
        now = DateTime.now()
        self.kwargs = {
            'start_date': DateTime(2000, 1, 1),
            'end_date': now
        }
        if isinstance(plan_time, DateTime):
            self.plan_time = plan_time
        else:
            self.plan_time = now

    def Cycle(self) -> Duration:
        return Duration(days=1)

    def ErrorCycle(self) -> Duration:
        return Duration(minutes=5)
