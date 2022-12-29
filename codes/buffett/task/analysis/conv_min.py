from buffett.adapter.pendulum import DateTime, Date
from buffett.analysis.study import ConvertStockMinuteAnalyst
from buffett.common.pendulum import DateSpan
from buffett.common.wrapper import Wrapper
from buffett.download.mysql import Operator
from buffett.task.base import Task


class ConvertStockMinuteTask(Task):
    def __init__(
        self, operator: Operator, datasource_op: Operator, start_time: DateTime
    ):
        super().__init__(
            wrapper=Wrapper(
                ConvertStockMinuteAnalyst(
                    ana_op=operator, stk_op=datasource_op
                ).calculate
            ),
            args=(DateSpan(start=Date(1990, 1, 1), end=Date(2022, 12, 1)),),
            start_time=start_time,
        )
        self._operator = operator
        self._datasource_op = datasource_op

    def get_subsequent_task(self, success: bool):
        return
