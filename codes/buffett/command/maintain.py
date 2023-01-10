from buffett.common.pendulum import DateTime
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.task.maintain import StockDailyMaintainTask, StockMinuteHfqMaintainTask
from buffett.task.maintain.min_bfq_mtain_task import StockMinuteBfqMaintainTask


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
    task_cls = [StockMinuteHfqMaintainTask]
    tasks = [
        task_cls[i](operator=ana_op, datasource_op=stk_op, start_time=DateTime.today())
        for i in range(len(task_cls))
    ]
    [x.run() for x in tasks]


def min_bfq_maintain():
    ana_rop = Operator(RoleType.DB_ANA)
    stk_rop = Operator(RoleType.DB_STK)
    mtain_wop = Operator(RoleType.DB_MT)
    task_cls = [StockMinuteBfqMaintainTask]
    tasks = [
        task_cls[i](
            ana_rop=ana_rop,
            stk_rop=stk_rop,
            mtain_wop=mtain_wop,
            start_time=DateTime.today(),
        )
        for i in range(len(task_cls))
    ]
    [x.run() for x in tasks]


if __name__ == "__main__":
    min_bfq_maintain()
