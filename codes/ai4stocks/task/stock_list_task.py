from pendulum import DateTime, Duration

from ai4stocks.download.akshare.stock_list_handler import StockListHandler
from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.task.download_task import DownloadTask


class StockListTask(DownloadTask):
    def __init__(
            self,
            plan_time: DateTime = None
    ):
        super().__init__(
            obj=StockListHandler(op=MysqlOperator(MysqlRole.DbStock)),
            method_name='download_and_save',
            plan_time=plan_time
        )

    def cycle(self) -> Duration:
        return Duration(days=15)

    def error_cycle(self) -> Duration:
        return Duration(minutes=5)
