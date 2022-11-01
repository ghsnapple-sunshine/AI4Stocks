import logging

from pandas import DataFrame

from buffett.common.pendelum import DateTime, Duration
from buffett.constants.col.stock import CODE, NAME
from buffett.download.fast import StockListHandler as SHandler
from buffett.task import StockDailyTask, StockMinuteTask
from test.common import Test


class TestDownload(Test):
    def setUp(self) -> None:
        super().setUp()
        # 初始化StockList
        data = [['000001', '平安银行'],
                ['600000', '浦发银行']]
        SHandler(operator=self.operator) \
            ._save_to_database(df=DataFrame(data=data, columns=[CODE, NAME]))

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
