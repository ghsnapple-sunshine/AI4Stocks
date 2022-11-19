from buffett.common.pendulum import DateTime
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.task import (
    TaskScheduler,
    StockListTask,
    StockDailyTask,
    StockMinuteTask,
    StockReformTask,
)


def download():
    operator = Operator(RoleType.DbStock)
    now = DateTime.now()
    sch = TaskScheduler(
        operator=operator,
        tasks=[
            StockListTask(operator=operator, start_time=now),
            StockDailyTask(operator=operator, start_time=now.add(seconds=1)),
            StockMinuteTask(operator=operator, start_time=now.add(seconds=2)),
            StockReformTask(operator=operator, start_time=now.add(seconds=3)),
        ],
    )
    sch.run()
