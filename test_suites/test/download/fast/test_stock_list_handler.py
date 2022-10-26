import unittest

from pandas import DataFrame

from ai4stocks.common.constants import STOCK_LIST_TABLE
from ai4stocks.download.fast.stock_list_handler import StockListHandler
from test.common.base_test import BaseTest


class StockListHandlerTest(BaseTest):
    def test_1(self):
        res = StockListHandler(self.op).download_and_save()
        cnt = self.op.get_record_cnt(STOCK_LIST_TABLE)
        assert cnt == res.shape[0]
        tbl = self.op.get_table(STOCK_LIST_TABLE)
        assert type(tbl) == DataFrame
        assert tbl['code'].apply(lambda x: x[0] != '4').all()

    def test_2(self):
        self.test_1()  # 重复执行，测试重复插入不报错


if __name__ == '__main__':
    unittest.main()
