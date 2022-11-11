import logging

from buffett.common.pendelum import DateTime
from buffett.download.mysql import Operator

from buffett.download.mysql.types import RoleType
from buffett.maintain import ReformMaintain
from buffett.task import TaskScheduler, StockReformTask, StockListTask, StockDailyTask, StockMinuteTask


def download():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='## %Y-%m-%d %H:%M:%S')
    now = DateTime.now()
    operator = Operator(RoleType.DbStock)
    """
    sch = TaskScheduler(operator=operator,
                        tasks=[StockListTask(operator=operator, start_time=now),
                               StockDailyTask(operator=operator, start_time=now.add(seconds=1)),
                               StockMinuteTask(operator=operator, start_time=now.add(seconds=2)),
                               StockReformTask(operator=operator, start_time=now.add(seconds=3))])
    sch.run()
    """
    ReformMaintain(operator=operator).run()
