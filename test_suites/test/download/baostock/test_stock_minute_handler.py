import unittest

from pendulum import DateTime

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
        stocks = create_stock_list_ex(self.op)
        start_date = DateTime(2002, 1, 1)
        end_date = DateTime(2003, 1, 1)
        tbls = StockMinuteHandler(op=self.op).download_and_save(start_time=start_date, end_time=end_date)
        assert stocks.shape[0] == len(tbls)


if __name__ == '__main__':
    unittest.main()
