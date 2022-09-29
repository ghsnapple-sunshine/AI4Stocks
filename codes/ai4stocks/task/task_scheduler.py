import time

from pendulum import DateTime

from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.task.base_task import BaseTask


class TaskScheduler:
    def __init__(
            self,
            op: MysqlOperator,
            tasks: list):
        self.op = op
        self.tasks = tasks

    def run(self):
        tasks = self.tasks
        while len(tasks) > 0:
            tasks = sorted(
                tasks,
                key=lambda x: x.PlanTime())
            now = DateTime.now()
            if now < tasks[0].PlanTime():
                print('{0}>>> 下一个任务预计将于{1}执行'.format(
                    now.format('YYYY-MM-DD HH:mm'),
                    tasks[0].plan_time.format('YYYY-MM-DD HH:mm')
                ))
                dly = tasks[0].plan_time - now
                time.sleep(secs=dly.seconds)
            status, res, new_task = tasks[0].Run()
            tasks = tasks[1:]
            if isinstance(new_task, BaseTask):
                tasks.append(new_task)
