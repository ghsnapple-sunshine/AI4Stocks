import unittest
from pandas import DataFrame
from ai4stocks.data_connect.mysql_common import MysqlColType, MysqlConstants, MysqlColAddReq
from test.common.base_test import BaseTest


class TestMysqlOperator(BaseTest):
    def test_all(self):
        self.CreateTable()
        self.InsertData()
        self.TryInsertData()
        self.GetTable()

    def setUp(self):
        super().setUp()
        self.op.DropTable(MysqlConstants.STOCK_LIST_TABLE)

    def CreateTable(self):
        data = [['code', MysqlColType.STOCK_CODE, MysqlColAddReq.PRIMKEY],
                ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]]
        df = DataFrame(data=data, columns=MysqlConstants.META_COLS)
        self.op.CreateTable(MysqlConstants.STOCK_LIST_TABLE, df, False)

    def InsertData(self):
        data = [['000001', '平安银行'],
                ['600000', '浦发银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.op.InsertData(MysqlConstants.STOCK_LIST_TABLE, df)

    def TryInsertData(self):
        data = [['000001', '狗狗银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.op.TryInsertData(MysqlConstants.STOCK_LIST_TABLE, df)  # 不更新数据
        db = self.op.GetTable(MysqlConstants.STOCK_LIST_TABLE)
        assert db[db['code'] == '000001'].iloc[0, 1] == '平安银行'

        self.op.TryInsertData(MysqlConstants.STOCK_LIST_TABLE, df, update=True)  # 更新数据
        db = self.op.GetTable(MysqlConstants.STOCK_LIST_TABLE)
        assert db[db['code'] == '000001'].iloc[0, 1] == '狗狗银行'

    def GetTable(self):
        tbl = self.op.GetTable(MysqlConstants.STOCK_LIST_TABLE)
        assert type(tbl) == DataFrame
        assert tbl.shape == (2, 2)


if __name__ == '__main__':
    unittest.main()
