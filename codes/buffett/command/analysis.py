from buffett.common.logger import Logger, LogType
from buffett.common.pendulum import DateTime
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.task.analysis import ConvertStockMinuteBfqTask, ConvertStockMinuteTask
from buffett.task.base import TaskScheduler


def analysis():
    ana_op = Operator(RoleType.DB_ANA)
    stk_op = Operator(RoleType.DB_STK)
    _analysis(
        task_cls=[
            # ConvertStockDailyTask,
            # FuquanFactorTask,
            ConvertStockMinuteBfqTask,
            ConvertStockMinuteTask,
            # TargetPatternRecognizeTask,
            # TargetStatZdfTask,
        ],
        ana_op=ana_op,
        stk_op=stk_op,
    )


def _analysis(task_cls, ana_op, stk_op):
    now = DateTime.now()
    tasks = [
        task_cls[i](
            operator=ana_op,
            datasource_op=stk_op,
            start_time=now.add(seconds=i),
        )
        for i in range(len(task_cls))
    ]
    sch = TaskScheduler(operator=ana_op, datasource_op=stk_op, tasks=tasks)
    sch.run()


if __name__ == "__main__":
    Logger.Level = LogType.INFO
    analysis()
