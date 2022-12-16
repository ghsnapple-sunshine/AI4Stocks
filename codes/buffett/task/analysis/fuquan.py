from buffett.adapter.pendulum import DateTime, Date
from buffett.analysis.study import FuquanAnalyst
from buffett.common.pendulum import DateSpan
from buffett.common.wrapper import Wrapper
from buffett.download.mysql import Operator
from buffett.task.base import Task


class FuquanFactorTask(Task):
    def __init__(
        self, operator: Operator, datasource_op: Operator, start_time: DateTime
    ):
        super().__init__(
            wrapper=Wrapper(
                FuquanAnalyst(operator=operator, datasource_op=datasource_op).calculate
            ),
            args=(DateSpan(start=Date(2000, 1, 1), end=Date(2021, 12, 31)),),
            start_time=start_time,
        )
        self._operator = operator
        self._datasource_op = datasource_op

    def get_subsequent_task(self, success: bool):
        return None
