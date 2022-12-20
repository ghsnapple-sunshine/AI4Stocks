from typing import Optional

from buffett.adapter.pendulum import DateTime
from buffett.common.wrapper import Wrapper
from buffett.download.maintain import StockDailyMaintain
from buffett.download.mysql import Operator
from buffett.task.base import Task


class StockDailyMaintainTask(Task):
    def __init__(
        self, operator: Operator, start_time: Optional[DateTime] = None, **kwargs
    ):
        super().__init__(
            wrapper=Wrapper(StockDailyMaintain(operator=operator).run),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        return None
