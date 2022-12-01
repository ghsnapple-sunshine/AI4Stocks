from buffett.common.pendulum import DateTime
from buffett.task import (
    StockPePbTask,
    ConceptConsTask,
    IndustryConsTask,
    ConceptDailyTask,
    IndexDailyTask,
)
from test import (
    Tester,
    create_1stock,
    create_1index,
    create_1concept,
    create_1industry,
)
from test.command.tools import create_task_no_subsequent


class TestMediumDownload(Tester):
    @classmethod
    def _setup_oncemore(cls):
        # 初始化
        create_1stock(operator=cls._operator)
        create_1index(operator=cls._operator)
        create_1concept(operator=cls._operator)
        create_1industry(operator=cls._operator)
        # 创建任务清单
        cls._task_cls = [
            create_task_no_subsequent(StockPePbTask),
            create_task_no_subsequent(ConceptConsTask),
            create_task_no_subsequent(ConceptDailyTask),
            create_task_no_subsequent(IndustryConsTask),
            create_task_no_subsequent(IndexDailyTask),
        ]

    def _setup_always(self) -> None:
        pass

    def test_download_medium(self):
        """
        download的镜像不循环测试
        (PEPB，概念板块成分股，概念板块日线，

        :return:
        """
        task_num = len(self._task_cls)
        operator = self._operator
        secs_before = DateTime.now().subtract(seconds=task_num)
        tasks = [
            self._task_cls[i](operator=operator, start_time=secs_before.add(seconds=i))
            for i in range(0, task_num)
        ]
        for t in tasks:
            t.run()
