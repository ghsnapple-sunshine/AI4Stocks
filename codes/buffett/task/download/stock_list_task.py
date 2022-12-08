from typing import Optional

from buffett.common.pendulum import DateTime
from buffett.common.wrapper import Wrapper
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class SseStockListTask(Task):
    def __init__(
        self, operator: Operator, start_time: Optional[DateTime] = None, **kwargs
    ):
        super().__init__(
            wrapper=Wrapper(SseStockListHandler(operator=operator).obtain_data),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return SseStockListTask(
                operator=self._operator, start_time=self._start_time.add(days=15)
            )
        else:
            return SseStockListTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )


class BsStockListTask(Task):
    def __init__(
        self, operator: Operator, start_time: Optional[DateTime] = None, **kwargs
    ):
        super().__init__(
            wrapper=Wrapper(BsStockListHandler(operator=operator).obtain_data),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return BsStockListTask(
                operator=self._operator, start_time=self._start_time.add(days=15)
            )
        else:
            return BsStockListTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
