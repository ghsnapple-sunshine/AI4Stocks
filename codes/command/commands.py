from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.task.stock_daily_task import StockDailyTask
from ai4stocks.task.stock_list_task import StockListTask
from ai4stocks.task.stock_minute_task import StockMinuteTask
from ai4stocks.task.task_scheduler import TaskScheduler


def download():
    sch = TaskScheduler(
        op=MysqlOperator(MysqlRole.DbStock),
        # tasks=[StockListTask(), StockDailyTask(), StockMinuteTask()]
        tasks=[StockMinuteTask()]
    )
    sch.run()
