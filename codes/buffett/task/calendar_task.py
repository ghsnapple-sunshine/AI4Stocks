from typing import Optional

from buffett.adapter.pendulum import DateTime
from buffett.common.wrapper import Wrapper
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class CalendarTask(Task):
    def __init__(self, operator: Operator, start_time: Optional[DateTime] = None):
        super().__init__(
            wrapper=Wrapper(CalendarHandler(operator=operator).obtain_data),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return CalendarTask(
                operator=self._operator, start_time=self._start_time.add(months=1)
            )
        else:
            return CalendarTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
