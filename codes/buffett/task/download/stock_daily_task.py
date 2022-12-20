from typing import Optional

from buffett.common.pendulum import Date, DateTime, convert_date
from buffett.common.wrapper import Wrapper
from buffett.download import Para
from buffett.download.handler.stock import DcDailyHandler, BsDailyHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class DcStockDailyTask(Task):
    def __init__(
        self, operator: Operator, start_time: Optional[DateTime] = None, **kwargs
    ):
        super().__init__(
            wrapper=Wrapper(DcDailyHandler(operator=operator).obtain_data),
            args=(Para().with_start_n_end(start=Date(1990, 1, 1), end=Date.today()),),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return DcStockDailyTask(
                operator=self._operator, start_time=self._start_time.add(days=1)
            )
        else:
            return DcStockDailyTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )


class BsStockDailyTask(Task):
    def __init__(
        self, operator: Operator, start_time: Optional[DateTime] = None, **kwargs
    ):
        super().__init__(
            wrapper=Wrapper(BsDailyHandler(operator=operator).obtain_data),
            args=(Para().with_start_n_end(start=Date(1990, 1, 1), end=Date.today()),),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return BsStockDailyTask(
                operator=self._operator,
                start_time=convert_date(self._start_time.add(days=1)),
            )
        else:
            return BsStockDailyTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
