import logging

from pandas import DataFrame

from ai4stocks.common.pendelum import DateTime, Duration
from ai4stocks.constants.col.stock import CODE, NAME
from ai4stocks.download.fast import StockListHandler as SHandler
from ai4stocks.task import StockDailyTask, StockMinuteTask
from test.common import BaseTest


class TestDownload(BaseTest):
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
