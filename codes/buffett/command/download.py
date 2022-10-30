import logging

from buffett.common.pendelum import DateTime, Duration
from buffett.download.mysql import RoleType, Operator
from buffett.task import StockDailyTask, StockListTask, StockMinuteTask, TaskScheduler


def download():
    logging.basicConfig(level=logging.INFO)
    now = DateTime.now()
    operator = Operator(RoleType.DbStock)
    sch = TaskScheduler(
        operator=operator,
        tasks=[
            StockListTask(operator=operator, start_time=now),
            StockDailyTask(operator=operator, start_time=now + Duration(seconds=1)),
            StockMinuteTask(operator=operator, start_time=now + Duration(seconds=2))]
    )
    sch.run()
