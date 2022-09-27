import unittest
from pandas import DataFrame

from ai4stocks.common.constants import META_COLS
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq
from test.common.base_test import BaseTest


class TestMysqlOperator(BaseTest):
    def test_all(self):
        meta = self.CreateTable()
        self.InsertData()
        self.TryInsertData(meta)
        self.GetTable()

    def CreateTable(self):
        data = [['code', MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
                ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]]
        meta = DataFrame(data=data, columns=META_COLS)
        self.op.CreateTable(self.table_name, meta, False)
        return meta

    def InsertData(self):
        data = [['000001', '平安银行'],
                ['600000', '浦发银行'],
                ['600001', '建设银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.op.InsertData(self.table_name, df)

    def TryInsertData(self, col_meta: DataFrame):
        data = [['000001', '狗狗银行'],
                ['600000', '猪猪银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.op.TryInsertData(self.table_name, df)  # 不更新数据
        db = self.op.GetTable(self.table_name)
        assert db[db['code'] == '000001'].iloc[0, 1] == '平安银行'
        assert db[db['code'] == '600000'].iloc[0, 1] == '浦发银行'
        assert db[db['code'] == '600001'].iloc[0, 1] == '建设银行'

        self.op.TryInsertData(self.table_name, df, col_meta=col_meta, update=True)  # 更新数据
        db = self.op.GetTable(self.table_name)
        assert db[db['code'] == '000001'].iloc[0, 1] == '狗狗银行'
        assert db[db['code'] == '600000'].iloc[0, 1] == '猪猪银行'
        assert db[db['code'] == '600001'].iloc[0, 1] == '建设银行'

    def GetTable(self):
        db = self.op.GetTable('test')
        assert type(db) == DataFrame
        assert db.empty


if __name__ == '__main__':
    unittest.main()
