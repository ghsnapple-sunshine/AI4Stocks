import unittest
from pandas import DataFrame

from ai4stocks.common.constants import META_COLS
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq
from test.common.base_test import BaseTest


class TestMysqlOperator(BaseTest):
    def test_all(self):
        meta = self.__create_table__()
        self.__insert_data__()
        self.__try_insert_data__(meta)
        self.__get_table__()

    def __create_table__(self):
        data = [['code', MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
                ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]]
        meta = DataFrame(data=data, columns=META_COLS)
        self.op.create_table(self.table_name, meta, False)
        return meta

    def __insert_data__(self):
        data = [['000001', '平安银行'],
                ['600000', '浦发银行'],
                ['600001', '建设银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.op.insert_data(self.table_name, df)

    def __try_insert_data__(self, col_meta: DataFrame):
        data = [['000001', '狗狗银行'],
                ['600000', '猪猪银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.op.try_insert_data(self.table_name, df)  # 不更新数据
        db = self.op.get_table(self.table_name)
        assert db[db['code'] == '000001'].iloc[0, 1] == '平安银行'
        assert db[db['code'] == '600000'].iloc[0, 1] == '浦发银行'
        assert db[db['code'] == '600001'].iloc[0, 1] == '建设银行'

        self.op.try_insert_data(self.table_name, df, col_meta=col_meta, update=True)  # 更新数据
        db = self.op.get_table(self.table_name)
        assert db[db['code'] == '000001'].iloc[0, 1] == '狗狗银行'
        assert db[db['code'] == '600000'].iloc[0, 1] == '猪猪银行'
        assert db[db['code'] == '600001'].iloc[0, 1] == '建设银行'

    def __get_table__(self):
        db = self.op.get_table('test')
        assert type(db) == DataFrame
        assert db.empty


if __name__ == '__main__':
    unittest.main()
