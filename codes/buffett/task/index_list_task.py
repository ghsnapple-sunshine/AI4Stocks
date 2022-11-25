from buffett.common.pendulum import DateTime
from buffett.common.wrapper import Wrapper
from buffett.download.handler.index import AkIndexListHandler
from buffett.download.mysql import Operator
from buffett.task.base import Task


class IndexListTask(Task):
    def __init__(self, operator: Operator, start_time: DateTime = None):
        super().__init__(
            wrapper=Wrapper(AkIndexListHandler(operator=operator).obtain_data),
            start_time=start_time,
        )
        self._operator = operator

    def get_subsequent_task(self, success: bool):
        if success:
            return IndexListTask(
                operator=self._operator, start_time=self._start_time.add(days=15)
            )
        else:
            return IndexListTask(
                operator=self._operator, start_time=self._start_time.add(minutes=5)
            )
