from typing import Optional

from buffett.adapter.pendulum import DateTime
from buffett.common.wrapper import Wrapper
from buffett.download.handler.stock import DcFhpgHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class StockFhpgTask(Task):
    def __init__(
        self, operator: Operator, start_time: Optional[DateTime] = None, **kwargs
    ):
        super().__init__(
            wrapper=Wrapper(DcFhpgHandler(operator=operator).obtain_data),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return StockFhpgTask(
                operator=self._operator, start_time=self._start_time.add(days=7)
            )
        else:
            return StockFhpgTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
