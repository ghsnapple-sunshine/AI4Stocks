import unittest

from ai4stocks.common.pendelum import Date
from ai4stocks.download import Para
from ai4stocks.download.slow.ak_stock_minute_handler import AkStockMinuteHandler
from test.common.base_test import BaseTest
from test.common.test_tools import create_stock_list_2


class AkStockMinuteHandlerTest(BaseTest):
    def test_download(self) -> None:
        stocks = create_stock_list_2(self.operator)
        para = Para().with_start_n_end(Date(2022, 9, 1), Date(2022, 9, 30))
        tbls = AkStockMinuteHandler(operator=self.operator).obtain_data(para=para)
        assert stocks.shape[0] == len(tbls)


if __name__ == '__main__':
    unittest.main()
