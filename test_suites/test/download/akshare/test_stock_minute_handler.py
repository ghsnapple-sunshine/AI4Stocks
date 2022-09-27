import unittest

from pendulum import DateTime

from ai4stocks.download.akshare.stock_minute_handler import StockMinuteHandler
from test.common.test_tools import TestTools
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper


class StockMinuteHandlerTest(BaseTest):
    def test_download(self) -> None:
        stocks = TestTools.CreateStockList_2(self.op)
        start_date = DateTime(2022, 9, 1)
        end_date = DateTime(2022, 9, 30)
        tbls = StockMinuteHandler(op=self.op).DownloadAndSave(start_date=start_date, end_date=end_date)
        assert stocks.shape[0] == len(tbls)


if __name__ == '__main__':
    unittest.main()
