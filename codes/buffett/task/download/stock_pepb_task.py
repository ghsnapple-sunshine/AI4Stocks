from typing import Optional

from buffett.adapter.pendulum import DateTime, Date
from buffett.common.pendulum import DateSpan
from buffett.common.wrapper import Wrapper
from buffett.download.handler.stock.lg_pepb import LgPePbHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class StockPePbTask(Task):
    def __init__(
        self, operator: Operator, start_time: Optional[DateTime] = None, **kwargs
    ):
        super().__init__(
            wrapper=Wrapper(LgPePbHandler(operator=operator).obtain_data),
            args=(DateSpan(start=Date(2000, 1, 1), end=Date.today()),),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return StockPePbTask(
                operator=self._operator, start_time=self._start_time.add(days=7)
            )
        else:
            return StockPePbTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
