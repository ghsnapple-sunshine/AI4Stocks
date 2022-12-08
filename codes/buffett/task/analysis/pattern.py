from buffett.adapter.pendulum import DateTime
from buffett.analysis.study.pattern import PatternAnalyst
from buffett.common.wrapper import Wrapper
from buffett.download.mysql import Operator
from buffett.task.base import Task


class TargetPatternRecognizeTask(Task):
    def __init__(
        self, operator: Operator, datasource_op: Operator, start_time: DateTime
    ):
        super().__init__(
            wrapper=Wrapper(
                PatternAnalyst(datasource_op=select_op, operator=insert_op).calculate
            ),
            start_time=start_time,
        )
        self._select_op = select_op
        self._insert_op = insert_op

    def get_subsequent_task(self, success: bool):
        return None
