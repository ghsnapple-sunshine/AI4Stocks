import time

from pendulum import DateTime

from ai4stocks.download.connect import MysqlOperator
from ai4stocks.task import BaseTask


class TaskScheduler:
    def __init__(
            self,
            op: MysqlOperator,
            tasks: list
    ):
        self.op = op
        self.tasks = tasks

    def run(self):
        tasks = self.tasks
        while len(tasks) > 0:
            tasks = sorted(
                tasks,
                key=lambda x: x.get_plan_time())
            now = DateTime.now()
            if now < tasks[0].get_plan_time():
                delay = tasks[0].get_plan_time() - now
                print('{0}>>>Next task will be executed in {1}(Start at: {2})'.format(
                    now.format('YYYY-MM-DD HH:mm'),
                    delay.in_words(),
                    tasks[0].get_plan_time().format('YYYY-MM-DD HH:mm')
                ))

                time.sleep(delay.total_seconds())
            status, res, new_task = tasks[0].run()
            tasks = tasks[1:]
            if isinstance(new_task, BaseTask):
                tasks.append(new_task)
