from typing import Optional

from buffett.adapter.pendulum import DateTime
from buffett.analysis.maintain import ConvertStockMinuteAnalystMaintain
from buffett.common.wrapper import Wrapper
from buffett.download.maintain import StockDailyMaintain
from buffett.download.mysql import Operator
from buffett.task.base import Task


class ConvertStockMinuteMaintainTask(Task):
    def __init__(
        self,
        operator: Operator,
        datasource_op: Operator,
        start_time: Optional[DateTime] = None,
        **kwargs
    ):
        super().__init__(
            wrapper=Wrapper(
                ConvertStockMinuteAnalystMaintain(
                    ana_op=operator, stk_rop=datasource_op
                ).run
            ),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        return None
