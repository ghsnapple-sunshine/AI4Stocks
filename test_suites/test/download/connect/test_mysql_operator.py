import unittest
from pandas import DataFrame
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlConstants, MysqlColAddReq
from test.common.base_test import BaseTest


class TestMysqlOperator(BaseTest):
    def setUp(self):
        super().setUp()
        self.op.DropTable(self.table_name)

    def tearDown(self) -> None:
        self.op.DropTable(self.table_name)
        super().tearDown()

    def test_all(self):
        self.CreateTable()
        self.InsertData()
        self.TryInsertData()
        self.GetTable()

    def CreateTable(self):
        data = [['code', MysqlColType.STOCK_CODE, MysqlColAddReq.PRIMKEY],
                ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]]
        df = DataFrame(data=data, columns=MysqlConstants.META_COLS)
        self.op.CreateTable(self.table_name, df, False)

    def InsertData(self):
        data = [['000001', '平安银行'],
                ['600000', '浦发银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.op.InsertData(self.table_name, df)

    def TryInsertData(self):
        data = [['000001', '狗狗银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.op.TryInsertData(self.table_name, df)  # 不更新数据
        db = self.op.GetTable(self.table_name)
        assert db[db['code'] == '000001'].iloc[0, 1] == '平安银行'

        self.op.TryInsertData(self.table_name, df, update=True)  # 更新数据
        db = self.op.GetTable(self.table_name)
        assert db[db['code'] == '000001'].iloc[0, 1] == '狗狗银行'

    def GetTable(self):
        tbl = self.op.GetTable(self.table_name)
        assert type(tbl) == DataFrame
        assert tbl.shape == (2, 2)


if __name__ == '__main__':
    unittest.main()
