from typing import Optional

from buffett.adapter.pendulum import DateTime
from buffett.analysis.maintain import StockMinuteHfqMaintain
from buffett.common.wrapper import Wrapper
from buffett.download.mysql import Operator
from buffett.task.base import Task


class StockMinuteHfqMaintainTask(Task):
    def __init__(
        self,
        operator: Operator,
        datasource_op: Operator,
        start_time: Optional[DateTime] = None,
        **kwargs
    ):
        super().__init__(
            wrapper=Wrapper(
                StockMinuteHfqMaintain(
                    ana_rop=operator, ana_wop=operator.copy(), stk_rop=datasource_op
                ).run
            ),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        return None