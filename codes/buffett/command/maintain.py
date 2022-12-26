from buffett.common.pendulum import DateTime
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.task.maintain.daily_mtain_task import StockDailyMaintainTask


def maintain():
    operator = Operator(RoleType.DbStock)
    task_cls = [StockDailyMaintainTask]
    tasks = [
        task_cls[i](operator=operator, start_time=DateTime.today())
        for i in range(0, len(task_cls))
    ]
    [x.run() for x in tasks]


if __name__ == "__main__":
    maintain()
