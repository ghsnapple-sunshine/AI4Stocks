import unittest

from pendulum import DateTime

from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.baostock.stock_minute_handler import StockMinuteHandler
from test.common.test_tools import create_stock_list_1, create_stock_list_2, create_stock_list_ex
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper


class StockMinuteHandlerTest(BaseTest):
    def test_download_1_month(self) -> None:
        DbSweeper.clean_up()
        stocks = create_stock_list_2(self.op)
        start_date = DateTime(2022, 8, 1)
        end_date = DateTime(2022, 8, 31)
        tbls = StockMinuteHandler(op=self.op).download_and_save(start_time=start_date, end_time=end_date)
        assert stocks.shape[0] == len(tbls)

    def test_download_1_year(self) -> None:
        DbSweeper.clean_up()
        stocks = create_stock_list_2(self.op)
        start_date = DateTime(2021, 1, 1)
        end_date = DateTime(2021, 12, 31)
        tbls = StockMinuteHandler(op=self.op).download_and_save(start_time=start_date, end_time=end_date)
        assert stocks.shape[0] == len(tbls)

    def test_download_10_year(self) -> None:
        DbSweeper.clean_up()
        stocks = create_stock_list_1(self.op)
        start_date = DateTime(2011, 1, 1)
        end_date = DateTime(2021, 12, 31)
        tbls = StockMinuteHandler(op=self.op).download_and_save(start_time=start_date, end_time=end_date)
        assert stocks.shape[0] == len(tbls)

    def test_download_irregular(self) -> None:
        DbSweeper.clean_up()
        stocks = create_stock_list_ex(self.op, StockCode('000795'))
        start_date = DateTime(2002, 1, 1)
        end_date = DateTime(2003, 1, 1)
        tbls = StockMinuteHandler(op=self.op).download_and_save(start_time=start_date, end_time=end_date)
        assert stocks.shape[0] == len(tbls)

    def test_downlaod_in_a_day(self) -> None:
        DbSweeper.clean_up()
        create_stock_list_ex(self.op, StockCode('000795'))
        hdl = StockMinuteHandler(op=self.op)
        # 正常下载数据
        start_time = DateTime(year=2022, month=9, day=30, hour=9)
        end_time = DateTime(year=2022, month=9, day=30, hour=17)
        hdl.download_and_save(start_time=start_time, end_time=end_time)
        # 下载收盘后的数据
        start_time = DateTime(year=2022, month=9, day=30, hour=17)
        end_time = DateTime(year=2022, month=9, day=30, hour=18)
        hdl.download_and_save(start_time=start_time, end_time=end_time)

    def test_download_xx(self) -> None:
        DbSweeper.clean_up()
        create_stock_list_ex(self.op, StockCode('301369'))
        hdl = StockMinuteHandler(op=self.op)
        start_time = DateTime(year=2000, month=1, day=1)
        end_time = DateTime.now()
        hdl.download_and_save(start_time=start_time, end_time=end_time)


if __name__ == '__main__':
    unittest.main()
