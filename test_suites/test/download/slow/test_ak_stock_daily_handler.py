import unittest

from ai4stocks.common.pendelum import Date, Duration
from ai4stocks.common.stock import Code
from ai4stocks.download import Para
from ai4stocks.download.slow.ak_stock_daily_handler import AkStockDailyHandler
from ai4stocks.download.types import FuquanType
from test.common.base_test import BaseTest
from test.common.test_tools import create_stock_list_1, create_stock_list_2


class AkStockDailyHandlerTest(BaseTest):
    def test_all(self) -> None:
        self._download1()
        self._download2()

    def _download1(self) -> None:
        stocks = create_stock_list_1(self.operator)
        hdl = AkStockDailyHandler(self.operator)
        hdl.obtain_data(para=Para()
                        .with_start_n_end(start=Date(2022, 1, 5), end=Date(2022, 1, 6)))
        db = hdl.get_data(para=Para()
                          .with_code(Code('000001')).with_fuquan(FuquanType.BFQ))
        assert db.shape[0] == 2  # 2022/1/5, 2022/1/6

        hdl.obtain_data(para=Para()
                        .with_start_n_end(start=Date(2022, 1, 5), end=Date(2022, 1, 7)))
        db = hdl.get_data(para=Para()
                          .with_code(Code('000001')).with_fuquan(FuquanType.BFQ))
        assert db.shape[0] == 3  # 2022/1/5, 2022/1/6, 2022/1/7

        hdl.obtain_data(para=Para()
                        .with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 7)))
        db = hdl.get_data(para=Para()
                          .with_code(Code('000001')).with_fuquan(FuquanType.BFQ))
        assert db.shape[0] == 4  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7

        hdl.obtain_data(para=Para()
                        .with_start_n_end(start=Date(2022, 1, 3), end=Date(2022, 1, 8)))
        db = hdl.get_data(para=Para()
                          .with_code(Code('000001')).with_fuquan(FuquanType.BFQ))
        assert db.shape[0] == 4  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/3为公休日, 2022/1/8为周六

        hdl.obtain_data(para=Para()
                        .with_start_n_end(start=Date(2022, 1, 3), end=Date(2022, 1, 10)))
        db = hdl.get_data(para=Para()
                          .with_code(Code('000001')).with_fuquan(FuquanType.BFQ))
        assert db.shape[0] == 5  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/10, 2022/1/3为公休日, 2022/1/8为周六

    def _download2(self) -> None:
        stocks = create_stock_list_2(self.operator)
        hdl = AkStockDailyHandler(self.operator)
        end_date = Date.today() - Duration(days=1)
        start_date = Date.today() - Duration(months=1)
        tbls = hdl.obtain_data(para=Para()
                               .with_start_n_end(start=start_date, end=end_date))
        assert stocks.shape[0] * 3 == len(tbls)


if __name__ == '__main__':
    unittest.main()
