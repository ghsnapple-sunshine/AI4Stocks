from typing import Optional

from buffett.common.pendulum import DateTime
from buffett.common.wrapper import Wrapper
from buffett.download.handler.reform import ReformHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class StockReformTask(Task):
    def __init__(
        self, operator: Operator, start_time: Optional[DateTime] = None, **kwargs
    ):
        super().__init__(
            wrapper=Wrapper(ReformHandler(operator=operator).reform_data),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return StockReformTask(
                operator=self._operator, start_time=self._start_time.add(days=1)
            )
        else:
            return StockReformTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
