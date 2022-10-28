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
            op=self.operator,
            tasks=[
                BaseTask(obj=InnerA(), method_name='run', plan_time=DateTime.now() - Duration(seconds=1)),
                BaseTask(obj=InnerB(), method_name='run', plan_time=DateTime.now() - Duration(seconds=2)),
                BaseTask(obj=InnerC('C'), method_name='run', plan_time=DateTime.now()),
                BaseTask(obj=InnerC('C'), method_name='run2', plan_time=DateTime.now())
            ]
        )
        sch.run()

    def test_delay(self):
        start = DateTime.now()
        sch = TaskScheduler(
            op=self.operator,
            tasks=[
                BaseTask(obj=InnerA(), method_name='run', plan_time=DateTime.now() + Duration(seconds=10)),
                BaseTask(obj=InnerB(), method_name='run', plan_time=DateTime.now() + Duration(seconds=20)),
                BaseTask(obj=InnerC('C'), method_name='run', plan_time=DateTime.now())
            ]
        )
        sch.run()
        end = DateTime.now()
        assert end - start >= Duration(seconds=20)  # 预期至少20s才能执行完
