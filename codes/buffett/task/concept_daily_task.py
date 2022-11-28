from buffett.common.pendulum import Date, DateTime, DateSpan
from buffett.common.wrapper import Wrapper
from buffett.download.handler.concept import DcConceptDailyHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class ConceptDailyTask(Task):
    def __init__(self, operator: Operator, start_time: DateTime = None):
        super().__init__(
            wrapper=Wrapper(DcConceptDailyHandler(operator=operator).obtain_data),
            args=(DateSpan(start=Date(2000, 1, 1), end=Date.today()),),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return ConceptDailyTask(
                operator=self._operator, start_time=self._start_time.add(days=1)
            )
        else:
            return ConceptDailyTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
