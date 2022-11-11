import pandas as pd
from pandas import DataFrame

from buffett.common.interface import Singleton
from buffett.common.pendelum import DateTime, Duration
from buffett.common.wrapper import Wrapper
from buffett.constants.col.task import ERR_MSG, TASK_ID, PARENT_ID, CLASS, MODULE, CREATE_TIME, START_TIME, END_TIME, \
    SUCCESS
from buffett.constants.magic import get_module_name, get_name
from buffett.constants.table import TASK_RCD
from buffett.download.mysql import Operator
from buffett.task.task import Task
from buffett.task.task_scheduler import TaskScheduler
from test import DbSweeper
from test.tester import Tester


class InnerA:
    charm = 'A'

    def run(self):
        return self.charm


class InnerB(InnerA):
    charm = 'B'


class InnerC(InnerA):
    def __init__(self,
                 charm: str):
        self.charm = charm


class InnerD:
    COUNT = 0

    def run(self):
        InnerD.COUNT += 1
        if InnerD.COUNT > 2:
            raise ValueError('error')


class OneOffTaskA(Task):
    wrapper = Wrapper(InnerA().run)

    def __init__(self,
                 operator: Operator = None,
                 start_time: DateTime = None):
        super(OneOffTaskA, self).__init__(wrapper=self.wrapper, start_time=start_time)

    def get_subsequent_task(self, success: bool):
        return None


class OneOffTaskB(OneOffTaskA):
    wrapper = Wrapper(InnerB().run)


class OneOffTaskC(OneOffTaskA):
    wrapper = Wrapper(InnerC('c').run)


class PerfectTask(OneOffTaskA):
    wrapper = Wrapper(InnerD().run)

    def get_subsequent_task(self, success: bool):
        return PerfectTask(start_time=self._start_time) if success else None


class TestTaskScheduler(Tester):
    def test_run_with_oneoff_task(self):
        """
        测试不会生成新Task的场景

        :return:
        """
        DbSweeper.cleanup()
        now = DateTime.now()
        task_infos = [[OneOffTaskA, now - Duration(seconds=1)],
                      [OneOffTaskB, now - Duration(seconds=2)],
                      [OneOffTaskC, now]]

        tasks = [x[0](start_time=x[1]) for x in task_infos]
        sch = TaskScheduler(operator=self.operator, tasks=tasks)
        sch.run()
        # actual
        actual = self.operator.select_data(name=TASK_RCD)
        del actual[CREATE_TIME], actual[START_TIME], actual[END_TIME]  # 这三列不参与比较
        # expect
        task_id = [1, 2, 3]
        parent_id = [None] * 3
        task_class = [get_name(x[0]) for x in task_infos]
        task_module = [get_module_name(x[0]) for x in task_infos]
        success = [1] * 3
        err = [None] * 3
        expect = DataFrame({TASK_ID: task_id,
                            PARENT_ID: parent_id,
                            CLASS: task_class,
                            MODULE: task_module,
                            SUCCESS: success,
                            ERR_MSG: err})

        cmp = pd.concat([actual, expect]).drop_duplicates(keep=False)
        assert cmp.empty

    def test_run_with_oneoff_task_n_delay(self):
        """
        测试Task的延期执行是否生效

        :return:
        """
        DbSweeper.cleanup()
        start = DateTime.now()
        sch = TaskScheduler(
            operator=self.operator,
            tasks=[OneOffTaskA(start_time=DateTime.now() + Duration(seconds=10)),
                   OneOffTaskB(start_time=DateTime.now() + Duration(seconds=20)),
                   OneOffTaskC(start_time=DateTime.now())])
        sch.run()
        end = DateTime.now()
        # delay为20s，但是实际只用了19.xs，原因是存入数据库时未保存microseconds，产生了误差。
        assert end - start >= Duration(seconds=19)

    def test_run_with_new_task_n_error(self):
        DbSweeper.cleanup()
        self._run_with_new_task()
        actual = self.operator.select_data(name=TASK_RCD)
        assert actual[actual[SUCCESS] == 0].shape[0] == 1
        assert actual.shape[0] == 5

    def test_run_2times(self):
        DbSweeper.cleanup()
        self._run_with_new_task()
        self._run_with_new_task()

    def _run_with_new_task(self):
        InnerD.COUNT = 0
        sch = TaskScheduler(
            operator=self.operator,
            tasks=[OneOffTaskA(start_time=DateTime.now() - Duration(seconds=1)),
                   OneOffTaskB(start_time=DateTime.now() - Duration(seconds=2)),
                   PerfectTask(start_time=DateTime.now())])
        sch.run()
