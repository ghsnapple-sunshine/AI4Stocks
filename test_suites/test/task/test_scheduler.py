from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col.task import (
    ERR_MSG,
    TASK_ID,
    PARENT_ID,
    CLASS,
    MODULE,
    CREATE_TIME,
    START_TIME,
    END_TIME,
    SUCCESS,
)
from buffett.common.constants.meta.handler import TASK_META
from buffett.common.constants.table import TASK_RCD
from buffett.common.magic import get_module_name, get_name
from buffett.common.pendulum import DateTime, Duration
from buffett.common.wrapper import Wrapper
from buffett.task.base import Task, TaskScheduler
from test import DbSweeper, Tester


class InnerA:
    charm = "A"

    def run(self):
        return self.charm


class InnerB(InnerA):
    charm = "B"


class InnerC(InnerA):
    def __init__(self, charm: str):
        self.charm = charm


class InnerD:
    COUNT = 0

    @staticmethod
    def run():
        InnerD.COUNT += 1
        if InnerD.COUNT > 2:
            raise ValueError("error")


class OneOffTaskA(Task):
    wrapper = Wrapper(InnerA().run)

    def __init__(self, start_time: DateTime = None, **kwargs):
        super(OneOffTaskA, self).__init__(wrapper=self.wrapper, start_time=start_time)

    def get_subsequent_task(self, success: bool):
        return None


class OneOffTaskB(OneOffTaskA):
    wrapper = Wrapper(InnerB().run)


class OneOffTaskC(OneOffTaskA):
    wrapper = Wrapper(InnerC("c").run)


class PerfectTask(OneOffTaskA):
    wrapper = Wrapper(InnerD().run)

    def get_subsequent_task(self, success: bool):
        return PerfectTask(start_time=self._start_time) if success else None


class ContinousTask(OneOffTaskA):
    COUNT = 0

    def get_subsequent_task(self, success: bool):
        if ContinousTask.COUNT < 1:
            ContinousTask.COUNT += 1
            return ContinousTask(start_time=self.start_time.add(minutes=5))
        return


class TestTaskScheduler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        DbSweeper.erase()

    def test_run_with_oneoff_task(self):
        """
        测试不会生成新Task的场景

        :return:
        """
        now = DateTime.now()
        task_infos = [
            [OneOffTaskA, now.subtract(seconds=1)],
            [OneOffTaskB, now.subtract(seconds=2)],
            [OneOffTaskC, now],
        ]

        tasks = [x[0](start_time=x[1]) for x in task_infos]
        sch = TaskScheduler(operator=self._operator, tasks=tasks)
        sch.run()
        # actual
        actual = self._operator.select_data(name=TASK_RCD, meta=TASK_META)
        del actual[CREATE_TIME], actual[START_TIME], actual[END_TIME]  # 这三列不参与比较
        # expect
        task_id = [1, 2, 3]
        parent_id = [None] * 3
        task_class = [get_name(x[0]) for x in task_infos]
        task_module = [get_module_name(x[0]) for x in task_infos]
        success = [1] * 3
        err = [None] * 3
        expect = DataFrame(
            {
                TASK_ID: task_id,
                PARENT_ID: parent_id,
                CLASS: task_class,
                MODULE: task_module,
                SUCCESS: success,
                ERR_MSG: err,
            }
        )

        # cmp = pd.concat([actual, expect]).drop_duplicates(keep=False)
        # assert cmp.empty
        assert self.compare_dataframe(actual, expect)

    def test_run_with_oneoff_task_n_delay(self):
        """
        测试Task的延期执行是否生效

        :return:
        """
        start = DateTime.now()
        sch = TaskScheduler(
            operator=self._operator,
            tasks=[
                OneOffTaskA(start_time=DateTime.now().add(seconds=2)),
                OneOffTaskB(start_time=DateTime.now().add(seconds=4)),
                OneOffTaskC(start_time=DateTime.now()),
            ],
        )
        sch.run()
        end = DateTime.now()
        # delay10s，但是实际只用了9.xs，原因是存入数据库时未保存microseconds，产生了误差。
        assert end - start >= Duration(seconds=3)

    def test_run_with_new_task_n_error(self):
        self._run_with_3tasks()
        actual = self._operator.select_data(name=TASK_RCD, meta=TASK_META)
        assert actual[actual[SUCCESS] == 0].shape[0] == 1
        assert actual.shape[0] == 5

    def test_run_2times(self):
        self._run_with_3tasks()
        self._run_with_3tasks()
        actual = self._operator.select_data(name=TASK_RCD, meta=TASK_META)
        assert actual.shape[0] == 10  # 20221128：创建case判断重复时，不再考虑已完成case，因此预期从5->10

    def test_run_2times_add_task(self):
        self._run_with_1task()
        actual = self._operator.select_data(name=TASK_RCD, meta=TASK_META)
        assert actual.shape[0] == 1
        self._run_with_3tasks()
        actual = self._operator.select_data(name=TASK_RCD, meta=TASK_META)
        assert actual.shape[0] == 6  # 20221128：创建case判断重复时，不再考虑已完成case，因此预期从5->6

    def _run_with_3tasks(self):
        InnerD.COUNT = 0
        sch = TaskScheduler(
            operator=self._operator,
            tasks=[
                OneOffTaskA(start_time=DateTime.now().subtract(seconds=0.1)),
                OneOffTaskB(start_time=DateTime.now().subtract(seconds=0.2)),
                PerfectTask(start_time=DateTime.now()),
            ],
        )
        sch.run()

    def _run_with_1task(self):
        InnerD.COUNT = 0
        sch = TaskScheduler(
            operator=self._operator,
            tasks=[OneOffTaskA(start_time=DateTime.now().subtract(seconds=0.1))],
        )
        sch.run()

    def test_add_task_fail(self):
        """
        测试由于当前已有未完成case不增加case

        :return:
        """
        ContinousTask.COUNT = 0
        sch = TaskScheduler(
            operator=self._operator,
            tasks=[
                ContinousTask(start_time=DateTime.now().subtract(seconds=2)),
                ContinousTask(start_time=DateTime.now().subtract(seconds=1)),
            ],
        )
        sch.run()
        task_num = self._operator.select_row_num(TASK_RCD)
        assert task_num == 2

    def test_add_task_success(self):
        """
        测试由于当前没有未完成case需增加case

        :return:
        """
        ContinousTask.COUNT = 0
        sch = TaskScheduler(
            operator=self._operator,
            tasks=[ContinousTask(start_time=DateTime.now().subtract(days=1))],
        )
        sch.run()
        task_num = self._operator.select_row_num(TASK_RCD)
        assert task_num == 2
