from buffett.common.pendulum import DateTime
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.task.analysis import TargetPatternRecognizeTask
from buffett.task.download import TaskScheduler


def analysis():
    select_op = Operator(RoleType.DbStock)
    insert_op = Operator(RoleType.DbAnaly)
    now = DateTime.now()
    task_cls = [
        TargetPatternRecognizeTask,
    ]
    tasks = [
        task_cls[i](
            select_op=select_op, insert_op=insert_op, start_time=now.add(seconds=i)
        )
        for i in range(0, len(task_cls))
    ]
    sch = TaskScheduler(operator=insert_op, tasks=tasks)
    sch.run()


if __name__ == "__main__":
    analysis()
