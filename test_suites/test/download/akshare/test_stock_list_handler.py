import unittest
from pandas import DataFrame

from ai4stocks.common.constants import STOCK_LIST_TABLE
from ai4stocks.download.akshare.stock_list_handler import StockListHandler
from test.common.base_test import BaseTest


class StockListHandlerTest(BaseTest):
    def test_all(self):
        res = StockListHandler(self.op).DownloadAndSave()
        cnt = self.op.GetTableCnt(STOCK_LIST_TABLE)
        assert cnt == res.shape[0]
        tbl = self.op.GetTable(STOCK_LIST_TABLE)
        assert type(tbl) == DataFrame

    def test_again(self):
        self.test_all()  # 重复执行，测试重复插入不报错


if __name__ == '__main__':
    unittest.main()
