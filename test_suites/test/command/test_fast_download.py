from buffett.adapter import logging
from buffett.common.pendelum import DateTime
from buffett.task import StockListTask
from test import Tester


class FastDownloadTest(Tester):
    def setUp(self) -> None:
        super().setUp()

    def test_download_fast(self):
        """
        download的镜像测试（股票清单、不循环）

        :return:
        """
        logging.basicConfig(level=logging.INFO)
        now = DateTime.now()
        operator = self.operator

        tasks = [StockListTask(operator=operator, start_time=now)]
        for task in tasks:
            task.run()
