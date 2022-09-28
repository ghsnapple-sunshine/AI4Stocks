from pendulum import DateTime, Duration

from ai4stocks.download.akshare.stock_list_handler import StockListHandler
from ai4stocks.download.baostock.stock_minute_handler import StockMinuteHandler
from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.task.download_task import DownloadTask


class StockListTask(DownloadTask):
    def __init__(
            self,
            plan_time: DateTime = None
    ):
        self.obj = StockListHandler(
            op=MysqlOperator(MysqlRole.DbStock)
        )
        self.method_name = 'DownloadAndSave'
        self.args = None
        self.kwargs = None
        if isinstance(plan_time, DateTime):
            self.plan_time = plan_time
        else:
            self.plan_time = DateTime.now()

    def Cycle(self) -> Duration:
        return Duration(days=15)

    def ErrorCycle(self) -> Duration:
        return Duration(minutes=5)
