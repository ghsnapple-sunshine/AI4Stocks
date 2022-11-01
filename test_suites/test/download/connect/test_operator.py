import unittest

from pandas import DataFrame

from buffett.constants.meta import META_COLS
from buffett.download.mysql.types import ColType, AddReqType
from test.tester import Tester


class TestOperator(Tester):
    def test_all(self):
        meta = self._create_table()
        self._insert_data()
        self._try_insert_data(meta)
        self._insert_null_data__()
        self._get_table()

    def _create_table(self):
        data = [['code', ColType.STOCK_CODE, AddReqType.KEY],
                ['name', ColType.STOCK_NAME, AddReqType.NONE]]
        meta = DataFrame(data=data, columns=META_COLS)
        self.operator.create_table(self.table_name, meta, False)
        return meta

    def _insert_data(self):
        data = [['000001', '平安银行'],
                ['600000', '浦发银行'],
                ['600001', '建设银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.operator.insert_data(self.table_name, df)

    def _try_insert_data(self, col_meta: DataFrame):
        data = [['000001', '狗狗银行'],
                ['600000', '猪猪银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.operator.try_insert_data(self.table_name, df)  # 不更新数据
        db = self.operator.get_data(self.table_name)
        assert db[db['code'] == '000001'].iloc[0, 1] == '平安银行'
        assert db[db['code'] == '600000'].iloc[0, 1] == '浦发银行'
        assert db[db['code'] == '600001'].iloc[0, 1] == '建设银行'

        self.operator.try_insert_data(self.table_name, df, meta=col_meta, update=True)  # 更新数据
        db = self.operator.get_data(self.table_name)
        assert db[db['code'] == '000001'].iloc[0, 1] == '狗狗银行'
        assert db[db['code'] == '600000'].iloc[0, 1] == '猪猪银行'
        assert db[db['code'] == '600001'].iloc[0, 1] == '建设银行'

    def _insert_null_data__(self):
        data = [['000002', 'xxx'],
                ['000003', None]]
        df = DataFrame(data=data, columns=['code', 'name'])
        self.operator.insert_data(self.table_name, df)
        assert True  # 预期：不报错

    def _get_table(self):
        db = self.operator.get_data('test')
        assert type(db) == DataFrame
        assert db.empty


if __name__ == '__main__':
    unittest.main()
