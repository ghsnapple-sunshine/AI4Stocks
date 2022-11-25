from buffett.common.pendulum import DateTime
from buffett.task import (
    StockPePbTask,
    ConceptConsTask,
    IndustryConsTask,
    ConceptDailyTask,
    IndexDailyTask,
    StockListTask,
)
from test import Tester, create_1stock, create_1index, create_1concept, create_1industry


class TestMediumDownload(Tester):
    @classmethod
    def _setup_oncemore(cls):
        create_1stock(operator=cls._operator)
        create_1index(operator=cls._operator)
        create_1concept(operator=cls._operator)
        create_1industry(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download_medium(self):
        """
        download的镜像不循环测试


        :return:
        """
        secs_before = DateTime.now().subtract(seconds=10)
        task_cls = [
            StockPePbTask,
            StockListTask,
            ConceptConsTask,
            ConceptDailyTask,
            IndustryConsTask,
            IndexDailyTask,
        ]
        tasks = [
            task_cls[i](operator=self._operator, start_time=secs_before.add(seconds=i))
            for i in range(0, len(task_cls))
        ]
        for task in tasks:
            task.run()
