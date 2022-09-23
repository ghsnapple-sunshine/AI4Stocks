import unittest

from ai4stocks.ak_download.stock_list_handler import StockListHandler
from ai4stocks.ak_download.stock_daily_handler import StockDailyHandler
from test.common.base_test import BaseTest


class StockDailyHandlerTest(BaseTest):
    def test_download(self):
        stock_num = StockListHandler.DownloadAndSave(self.op)
        tbls = StockDailyHandler.DownAndSave(self.op)
        assert tbls == 2 * stock_num
        for tbl in tbls:
            self.op.DropTable(tbl)


if __name__ == '__main__':
    unittest.main()
