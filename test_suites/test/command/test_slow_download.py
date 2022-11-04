import logging

from buffett.common.pendelum import DateTime, Date
from buffett.download import Para
from buffett.task import StockDailyTask, StockMinuteTask, StockReformTask, StockListTask
from test import Tester, create_2stocks, DbSweeper


class SlowDownloadTest(Tester):
    def test_download(self):
        """
        download的镜像测试

        :return:
        """
        # 初始化StockList
        DbSweeper.cleanup()
        create_2stocks(operator=self.operator)
        logging.basicConfig(level=logging.INFO)
        now = DateTime.now()
        operator = self.operator
        tasks = [StockDailyTask(operator=operator, start_time=now.add(seconds=1)),
                 StockMinuteTask(operator=operator, start_time=now.add(seconds=2)),
                 StockReformTask(operator=operator, start_time=now.add(seconds=3))]
        for task in tasks:
            task.run()
