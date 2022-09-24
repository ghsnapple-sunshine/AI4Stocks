import unittest
from pandas import DataFrame

from ai4stocks.download.akshare.stock_list_handler import StockListHandler
from ai4stocks.download.connect.mysql_common import MysqlConstants
from test.common.base_test import BaseTest


class StockListHandlerTest(BaseTest):
    def test_all(self):
        res = StockListHandler.DownloadAndSave(self.op)
        cnt = self.op.GetTableCnt(MysqlConstants.STOCK_LIST_TABLE)
        assert cnt == res.shape[0]
        tbl = self.op.GetTable(MysqlConstants.STOCK_LIST_TABLE)
        assert type(tbl) == DataFrame

    def test_again(self):
        self.test_all() # 重复执行，测试重复插入不报错

    def tearDown(self) -> None:
        self.op.DropTable(MysqlConstants.STOCK_LIST_TABLE)


if __name__ == '__main__':
    unittest.main()
