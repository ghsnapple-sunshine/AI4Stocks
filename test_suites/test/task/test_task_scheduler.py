from pendulum import DateTime, Duration

from ai4stocks.task.base_task import BaseTask
from ai4stocks.task.task_scheduler import TaskScheduler
from test.common.base_test import BaseTest


class InnerA:
    def __init__(self):
        self.charm = 'A'

    def run(self):
        return self.charm


class InnerB:
    def __init__(self):
        self.charm = 'B'

    def run(self):
        return self.charm


class InnerC:
    def __init__(
            self,
            charm: str
    ):
        self.charm = charm

    def run(self):
        return self.charm


class TestTaskScheduler(BaseTest):
    def test_all(self):
        sch = TaskScheduler(
            op=self.op,
            tasks=[
                BaseTask(obj=InnerA(), method_name='run', plan_time=DateTime.now() - Duration(minutes=1)),
                BaseTask(obj=InnerB(), method_name='run', plan_time=DateTime.now() - Duration(minutes=2)),
                BaseTask(obj=InnerC('C'), method_name='run', plan_time=DateTime.now()),
                BaseTask(obj=InnerC('C'), method_name='run2', plan_time=DateTime.now())
            ]
        )
        sch.run()
