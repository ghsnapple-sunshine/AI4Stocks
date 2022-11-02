import logging

from buffett.common.pendelum import DateTime, Duration
from buffett.task import StockDailyTask, StockMinuteTask
from test import Tester, create_2stocks


class SlowDownloadTest(Tester):
    def setUp(self) -> None:
        super().setUp()
        # 初始化StockList
        create_2stocks(operator=self.operator)

    def test_download(self):
        """
        download的镜像测试

        :return:
        """
        logging.basicConfig(level=logging.INFO)
        now = DateTime.now()
        operator = self.operator
        tasks = [StockDailyTask(operator=operator, start_time=now + Duration(seconds=1)),
                 StockMinuteTask(operator=operator, start_time=now + Duration(seconds=2))]
        for task in tasks:
            task.run()
