import unittest

from pendulum import DateTime, Duration

from ai4stocks.common.types import FuquanType
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.slow.ak_stock_daily_handler import AkStockDailyHandler
from test.common.test_tools import create_stock_list_1, create_stock_list_2
from test.common.base_test import BaseTest


class AkStockDailyHandlerTest(BaseTest):
    def test_all(self) -> None:
        self.__download1__()
        self.__download2__()

    def __download1__(self) -> None:
        stocks = create_stock_list_1(self.op)
        hdl = AkStockDailyHandler(self.op)
        hdl.download_and_save(start_time=DateTime(2022, 1, 5), end_time=DateTime(2022, 1, 6))
        db = hdl.get_table(StockCode('000001'), fuquan=FuquanType.NONE)
        assert db.shape[0] == 2  # 2022/1/5, 2022/1/6

        hdl.download_and_save(start_time=DateTime(2022, 1, 5), end_time=DateTime(2022, 1, 7))
        db = hdl.get_table(StockCode('000001'), fuquan=FuquanType.NONE)
        assert db.shape[0] == 3  # 2022/1/5, 2022/1/6, 2022/1/7

        hdl.download_and_save(start_time=DateTime(2022, 1, 4), end_time=DateTime(2022, 1, 6))
        db = hdl.get_table(StockCode('000001'), fuquan=FuquanType.NONE)
        assert db.shape[0] == 4  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7

        hdl.download_and_save(start_time=DateTime(2022, 1, 3), end_time=DateTime(2022, 1, 8))
        db = hdl.get_table(StockCode('000001'), fuquan=FuquanType.NONE)
        assert db.shape[0] == 4  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/3为公休日, 2022/1/8为周六

        hdl.download_and_save(start_time=DateTime(2022, 1, 3), end_time=DateTime(2022, 1, 10))
        db = hdl.get_table(StockCode('000001'), fuquan=FuquanType.NONE)
        assert db.shape[0] == 5  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/10, 2022/1/3为公休日, 2022/1/8为周六

    def __download2__(self) -> None:
        stocks = create_stock_list_2(self.op)
        hdl = AkStockDailyHandler(self.op)
        end_date = DateTime.now() - Duration(days=1)
        start_date = DateTime.now() - Duration(months=1)
        tbls = hdl.download_and_save(start_time=start_date, end_time=end_date)
        assert stocks.shape[0] * 3 == len(tbls)


if __name__ == '__main__':
    unittest.main()
