from buffett.common.pendulum import Date, DateTime
from buffett.common.wrapper import Wrapper
from buffett.download import Para
from buffett.download.handler.industry import AkIndustryDailyHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class IndustryDailyTask(Task):
    def __init__(self, operator: Operator, start_time: DateTime = None):
        super().__init__(
            wrapper=Wrapper(AkIndustryDailyHandler(operator=operator).obtain_data),
            args=(Para().with_start_n_end(start=Date(2000, 1, 1), end=Date.today()),),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return IndustryDailyTask(
                operator=self._operator, start_time=self._start_time.add(days=1)
            )
        else:
            return IndustryDailyTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
