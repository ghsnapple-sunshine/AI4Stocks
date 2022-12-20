from buffett.common.logger import Logger, LogType
from buffett.common.pendulum import DateTime
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.task.analysis import (
    TargetPatternRecognizeTask,
    TargetStatZdfTask,
    FuquanFactorTask,
    ConvertStockMinuteTask,
)
from buffett.task.base import TaskScheduler


def analysis():
    operator = Operator(RoleType.DbAnaly)
    datasource_op = Operator(RoleType.DbStock)
    now = DateTime.now()
    task_cls = [
        # TargetPatternRecognizeTask,
        # TargetStatZdfTask,
        # FuquanFactorTask,
        ConvertStockMinuteTask,
    ]
    tasks = [
        task_cls[i](
            operator=operator,
            datasource_op=datasource_op,
            start_time=now.add(seconds=i),
        )
        for i in range(0, len(task_cls))
    ]
    sch = TaskScheduler(operator=operator, datasource_op=datasource_op, tasks=tasks)
    sch.run()


if __name__ == "__main__":
    # Logger.Level = LogType.DEBUG
    analysis()
