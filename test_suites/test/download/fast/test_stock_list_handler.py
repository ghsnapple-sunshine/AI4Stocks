import unittest

from pandas import DataFrame

from ai4stocks.constants.table import STK_LS
from ai4stocks.download.fast.stock_list_handler import StockListHandler
from test.common.base_test import BaseTest


class StockListHandlerTest(BaseTest):
    def test_1(self):
        res = StockListHandler(self.operator).obtain_data()
        cnt = self.operator.get_record_cnt(STK_LS)
        assert cnt == res.shape[0]
        tbl = self.operator.get_table(STK_LS)
        assert type(tbl) == DataFrame
        assert tbl['code'].apply(lambda x: x[0] != '4').all()

    def test_2(self):
        self.test_1()  # 重复执行，测试重复插入不报错


if __name__ == '__main__':
    unittest.main()
