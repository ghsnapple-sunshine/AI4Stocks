from buffett.adapter.pendulum import DateTime, Date
from buffett.common.wrapper import Wrapper
from buffett.download import Para
from buffett.download.handler.stock import DcDividendHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class StockDividendTask(Task):
    def __init__(self, operator: Operator, start_time: DateTime = None):
        super().__init__(
            wrapper=Wrapper(DcDividendHandler(operator=operator).obtain_data),
            args=(Para().with_start_n_end(start=Date(2000, 1, 1), end=Date.today()),),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return StockDividendTask(
                operator=self._operator, start_time=self._start_time.add(days=7)
            )
        else:
            return StockDividendTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
