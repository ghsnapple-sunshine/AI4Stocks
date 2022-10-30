import logging
import time

from buffett.common.pendelum import DateTime
from buffett.download.mysql import Operator
from buffett.task import Task


class TaskScheduler:
    def __init__(self, operator: Operator, tasks: list[Task]):
        self._operator = operator
        self._tasks = tasks

    def run(self):
        tasks = self._tasks
        while len(tasks) > 0:
            tasks = sorted(tasks, key=lambda x: x.start_time)
            now = DateTime.now()
            if now < tasks[0].start_time:
                delay = tasks[0].start_time - now
                logging.info('{0}>>>Next task will be executed in {1}(Start at: {2})'.format(
                    now.format('YYYY-MM-DD HH:mm'),
                    delay.in_words(),
                    tasks[0].start_time.format('YYYY-MM-DD HH:mm')))

                time.sleep(delay.total_seconds())
            status, res, new_task = tasks[0].run()
            tasks = tasks[1:]
            if isinstance(new_task, Task):
                tasks.append(new_task)
