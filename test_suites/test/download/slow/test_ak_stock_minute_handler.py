import unittest

from pendulum import DateTime

from ai4stocks.download.slow.ak_stock_minute_handler import AkStockMinuteHandler
from test.common.test_tools import create_stock_list_2
from test.common.base_test import BaseTest


class AkStockMinuteHandlerTest(BaseTest):
    def test_download(self) -> None:
        stocks = create_stock_list_2(self.op)
        start_date = DateTime(2022, 9, 1)
        end_date = DateTime(2022, 9, 30)
        tbls = AkStockMinuteHandler(operator=self.op).download_and_save(start_time=start_date, end_time=end_date)
        assert stocks.shape[0] == len(tbls)


if __name__ == '__main__':
    unittest.main()
