import unittest

from pendulum import DateTime

from ai4stocks.download.akshare.stock_minute_handler import StockMinuteHandler
from test.download.ak_download.test_tools import TestTools
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper


class StockMinuteHandlerTest(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        DbSweeper.CleanUp()

    def test_download(self) -> None:
        stocks = TestTools.CreateStockListTable(self.op)
        start_date = DateTime(2022, 9, 1)
        end_date = DateTime(2022, 9, 30)
        tbls = StockMinuteHandler.DownloadAndSave(op=self.op, start_date=start_date, end_date=end_date)
        assert stocks.shape[0] * 2 == len(tbls)


if __name__ == '__main__':
    unittest.main()
