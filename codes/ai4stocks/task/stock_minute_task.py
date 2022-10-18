from pendulum import DateTime, Duration

from ai4stocks.download.baostock import StockMinuteHandler
from ai4stocks.download.connect import MysqlOperator, MysqlRole
from ai4stocks.task.download_task import DownloadTask


class StockMinuteTask(DownloadTask):
    def __init__(
            self,
            plan_time: DateTime = None
    ):
        super().__init__(
            obj=StockMinuteHandler(op=MysqlOperator(MysqlRole.DbStock)),
            method_name='download_and_save',
            kwargs={
                'start_time': DateTime(year=2020, month=1, day=1),
                'end_time': DateTime.now()
            },
            plan_time=plan_time
        )

    def cycle(self) -> Duration:
        return Duration(days=1)

    def error_cycle(self) -> Duration:
        return Duration(minutes=5)
