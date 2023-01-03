from buffett.common.pendulum import DateTime
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.task.maintain import StockDailyMaintainTask, ConvertStockMinuteMaintainTask


def maintain():
    operator = Operator(RoleType.DB_STK)
    task_cls = [StockDailyMaintainTask]
    tasks = [
        task_cls[i](operator=operator, start_time=DateTime.today())
        for i in range(len(task_cls))
    ]
    [x.run() for x in tasks]


def analysis_maintain():
    ana_op = Operator(RoleType.DB_ANA)
    stk_op = Operator(RoleType.DB_STK)
    task_cls = [ConvertStockMinuteMaintainTask]
    tasks = [
        task_cls[i](operator=ana_op, datasource_op=stk_op, start_time=DateTime.today())
        for i in range(len(task_cls))
    ]
    [x.run() for x in tasks]


if __name__ == "__main__":
    analysis_maintain()
