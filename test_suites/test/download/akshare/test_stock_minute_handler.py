import unittest

from pendulum import DateTime

from ai4stocks.download.akshare.stock_minute_handler import StockMinuteHandler
from test.common.test_tools import create_stock_list_2
from test.common.base_test import BaseTest


class StockMinuteHandlerTest(BaseTest):
    def test_download(self) -> None:
        stocks = create_stock_list_2(self.op)
        start_date = DateTime(2022, 9, 1)
        end_date = DateTime(2022, 9, 30)
        tbls = StockMinuteHandler(op=self.op).DownloadAndSave(start_time=start_date, end_time=end_date)
        assert stocks.shape[0] == len(tbls)


if __name__ == '__main__':
    unittest.main()
