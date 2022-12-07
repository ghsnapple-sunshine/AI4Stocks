from buffett.adapter.pendulum import DateTime
from buffett.common.wrapper import Wrapper
from buffett.download.handler.stock import DcProfitHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class StockProfitTask(Task):
    def __init__(self, operator: Operator, start_time: DateTime = None):
        super().__init__(
            wrapper=Wrapper(DcProfitHandler(operator=operator).obtain_data),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return StockProfitTask(
                operator=self._operator, start_time=self._start_time.add(days=15)
            )
        else:
            return StockProfitTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
