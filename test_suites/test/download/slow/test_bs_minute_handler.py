import unittest

from buffett.common.pendelum import Date, DateTime
from buffett.common.stock import Code
from buffett.download import Para
from buffett.download.slow.bs_minute_handler import BsMinuteHandler
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper
from test.common.test_tools import create_stock_list_1, create_stock_list_2, create_stock_list_ex


class BsStockMinuteHandlerTest(BaseTest):
    def test_download_1_month(self) -> None:
        DbSweeper.cleanup()
        stocks = create_stock_list_2(self.operator)
        para = Para().with_start_n_end(Date(2022, 8, 1), Date(2022, 8, 31))
        tbls = BsMinuteHandler(operator=self.operator).obtain_data(para=para)
        assert stocks.shape[0] == len(tbls)

    def test_download_1_year(self) -> None:
        DbSweeper.cleanup()
        stocks = create_stock_list_2(self.operator)
        para = Para().with_start_n_end(Date(2021, 1, 1), Date(2021, 12, 31))
        tbls = BsMinuteHandler(operator=self.operator).obtain_data(para)
        assert stocks.shape[0] == len(tbls)

    def test_download_10_year(self) -> None:
        DbSweeper.cleanup()
        stocks = create_stock_list_1(self.operator)
        para = Para().with_start_n_end(Date(2021, 1, 1), Date(2021, 12, 31))
        tbls = BsMinuteHandler(operator=self.operator).obtain_data(para)
        assert stocks.shape[0] == len(tbls)

    def test_download_irregular(self) -> None:
        DbSweeper.cleanup()
        stocks = create_stock_list_ex(self.operator, Code('000795'))
        para = Para().with_start_n_end(Date(2002, 1, 1), Date(2003, 1, 1))
        tbls = BsMinuteHandler(operator=self.operator).obtain_data(para=para)
        assert stocks.shape[0] == len(tbls)

    def test_downlaod_in_a_day(self) -> None:
        DbSweeper.cleanup()
        create_stock_list_ex(self.operator, Code('000795'))
        hdl = BsMinuteHandler(operator=self.operator)
        # 正常下载数据
        para = Para().with_start_n_end(start=DateTime(year=2022, month=9, day=30, hour=9),
                                       end=DateTime(year=2022, month=9, day=30, hour=17))
        hdl.obtain_data(para=para)
        # 下载收盘后的数据
        para = Para().with_start_n_end(start=DateTime(year=2022, month=9, day=30, hour=17),
                                       end=DateTime(year=2022, month=9, day=30, hour=18))
        hdl.obtain_data(para=para)

    def test_download_xx(self) -> None:
        DbSweeper.cleanup()
        create_stock_list_ex(self.operator, Code('301369'))
        hdl = BsMinuteHandler(operator=self.operator)
        para = Para().with_start_n_end(Date(2000, 1, 1), Date.today())
        hdl.obtain_data(para=para)


if __name__ == '__main__':
    unittest.main()
