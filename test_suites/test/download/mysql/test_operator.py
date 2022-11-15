from buffett.adapter.pandas import DataFrame, pd
from buffett.constants.col.stock import CODE, NAME
from buffett.constants.meta import META_COLS
from buffett.download.mysql.types import ColType, AddReqType
from test import Tester


class TestOperator(Tester):
    def test_all(self):
        meta = self._create_table()
        self._insert_data()
        self._try_insert_data(meta)
        self._insert_null_data()
        self._get_table()

    def _create_table(self):
        data = [[CODE, ColType.STOCK_CODE_NAME, AddReqType.KEY],
                [NAME, ColType.STOCK_CODE_NAME, AddReqType.NONE]]
        meta = DataFrame(data=data, columns=META_COLS)
        self.operator.create_table(self.table_name, meta, False)
        return meta

    def _insert_data(self):
        data = [['000001', '平安银行'],
                ['600000', '浦发银行'],
                ['600001', '建设银行']]
        df = DataFrame(data=data, columns=[CODE, NAME])
        self.operator.insert_data(self.table_name, df)
        self._validate(data)

    def _try_insert_data(self,
                         meta: DataFrame):
        data = [['000001', '狗狗银行'],
                ['600000', '猪猪银行']]
        df = DataFrame(data=data, columns=[CODE, NAME])
        self.operator.try_insert_data(self.table_name, df)  # 不更新数据
        ass_data = [['000001', '平安银行'],
                    ['600000', '浦发银行'],
                    ['600001', '建设银行']]
        self._validate(ass_data)

        self.operator.try_insert_data(self.table_name, df, meta=meta, update=True)  # 更新数据
        ass_data = [['000001', '狗狗银行'],
                    ['600000', '猪猪银行'],
                    ['600001', '建设银行']]
        self._validate(ass_data)

    def _insert_null_data(self):
        data = [['000002', 'xxx'],
                ['000003', None]]
        df = DataFrame(data=data, columns=[CODE, NAME])
        self.operator.insert_data(self.table_name, df)
        assert True  # 预期：不报错

    def _validate(self,
                  ass_data: list) -> None:
        db = self.operator.select_data(self.table_name)
        ass_df = DataFrame(data=ass_data, columns=[CODE, NAME])
        cmp = pd.concat([db, ass_df]).drop_duplicates(keep=False)
        assert cmp.empty

    def _get_table(self):
        db = self.operator.select_data('test')
        assert type(db) == DataFrame
        assert db.empty
