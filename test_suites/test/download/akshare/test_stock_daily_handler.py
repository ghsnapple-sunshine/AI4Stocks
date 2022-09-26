import unittest

from pendulum import DateTime

from ai4stocks.download.akshare.stock_daily_handler import StockDailyHandler
from test.common.test_tools import TestTools
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper


class StockDailyHandlerTest(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        DbSweeper.CleanUp()

    def test_download(self) -> None:
        stocks = TestTools.CreateStockListTable(self.op)
        start_date = DateTime(2022, 1, 1)
        end_date = DateTime(2022, 6, 30)
        tbls = StockDailyHandler(self.op).DownloadAndSave(start_date=start_date, end_date=end_date)
        assert stocks.shape[0] * 3 == len(tbls)


if __name__ == '__main__':
    unittest.main()
