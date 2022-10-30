from buffett.common.pendelum import DateTime, Duration
from buffett.task.task import Task
from buffett.task.task_scheduler import TaskScheduler
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
            operator=self.operator,
            tasks=[Task(attr=InnerA().run, start_time=DateTime.now() - Duration(seconds=1)),
                   Task(attr=InnerB().run, start_time=DateTime.now() - Duration(seconds=2)),
                   Task(attr=InnerC('C').run, start_time=DateTime.now())])
        sch.run()

    def test_delay(self):
        start = DateTime.now()
        sch = TaskScheduler(
            operator=self.operator,
            tasks=[Task(attr=InnerA().run, start_time=DateTime.now() + Duration(seconds=10)),
                   Task(attr=InnerB().run, start_time=DateTime.now() + Duration(seconds=20)),
                   Task(attr=InnerC('C').run, start_time=DateTime.now())])
        sch.run()
        end = DateTime.now()
        assert end - start >= Duration(seconds=20)  # 预期至少20s才能执行完
