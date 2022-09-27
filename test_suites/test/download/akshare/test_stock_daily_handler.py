import unittest

from pendulum import DateTime

from ai4stocks.common.types import FuquanType
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.akshare.stock_daily_handler import StockDailyHandler
from test.common.test_tools import TestTools
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper


class StockDailyHandlerTest(BaseTest):
    def test_all(self) -> None:
        self.Download1()
        self.Download2()

    def Download2(self) -> None:
        stocks = TestTools.CreateStockList_2(self.op)
        hdl = StockDailyHandler(self.op)
        tbls = hdl.DownloadAndSave(start_date=DateTime(2022, 1, 1), end_date=DateTime(2022, 2, 10))
        assert stocks.shape[0] * 3 == len(tbls)

    def Download1(self) -> None:
        TestTools.CreateStockList_1(self.op)
        hdl = StockDailyHandler(self.op)
        hdl.DownloadAndSave(start_date=DateTime(2022, 1, 5), end_date=DateTime(2022, 1, 6))
        db = hdl.GetTable(StockCode('000001'), fuquan=FuquanType.NONE)
        assert db.shape[0] == 2  # 2022/1/5, 2022/1/6

        hdl.DownloadAndSave(start_date=DateTime(2022, 1, 5), end_date=DateTime(2022, 1, 7))
        db = hdl.GetTable(StockCode('000001'), fuquan=FuquanType.NONE)
        assert db.shape[0] == 3  # 2022/1/5, 2022/1/6, 2022/1/7

        hdl.DownloadAndSave(start_date=DateTime(2022, 1, 4), end_date=DateTime(2022, 1, 6))
        db = hdl.GetTable(StockCode('000001'), fuquan=FuquanType.NONE)
        assert db.shape[0] == 4  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7

        hdl.DownloadAndSave(start_date=DateTime(2022, 1, 3), end_date=DateTime(2022, 1, 8))
        db = hdl.GetTable(StockCode('000001'), fuquan=FuquanType.NONE)
        assert db.shape[0] == 4  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/3为公休日, 2022/1/8为周六

        hdl.DownloadAndSave(start_date=DateTime(2022, 1, 3), end_date=DateTime(2022, 1, 10))
        db = hdl.GetTable(StockCode('000001'), fuquan=FuquanType.NONE)
        assert db.shape[0] == 5  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/10, 2022/1/3为公休日, 2022/1/8为周六


if __name__ == '__main__':
    unittest.main()
