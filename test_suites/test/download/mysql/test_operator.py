from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col.meta import META_COLS
from buffett.common.constants.col.stock import CODE, NAME
from buffett.download.mysql.types import ColType, AddReqType
from test import Tester, DbSweeper


class TestOperator(Tester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_all(self):
        meta = self._create_table()
        self._insert_data()
        self._try_insert_data(meta)
        self._insert_null_data()
        self._select_data()

    def _create_table(self):
        data = [
            [CODE, ColType.CODE, AddReqType.KEY],
            [NAME, ColType.CODE, AddReqType.NONE],
        ]
        meta = DataFrame(data=data, columns=META_COLS)
        self._operator.create_table(self._table_name, meta, False)
        return meta

    def _insert_data(self):
        data = [["000001", "平安银行"], ["600000", "浦发银行"], ["600001", "建设银行"]]
        df = DataFrame(data=data, columns=[CODE, NAME])
        self._operator.insert_data(self._table_name, df)
        self._validate(data)

    def _try_insert_data(self, meta: DataFrame):
        data = [["000001", "狗狗银行"], ["600000", "猪猪银行"]]
        df = DataFrame(data=data, columns=[CODE, NAME])
        self._operator.try_insert_data(self._table_name, df)  # 不更新数据
        ass_data = [["000001", "平安银行"], ["600000", "浦发银行"], ["600001", "建设银行"]]
        self._validate(ass_data)

        self._operator.try_insert_data(
            self._table_name, df, meta=meta, update=True
        )  # 更新数据
        ass_data = [["000001", "狗狗银行"], ["600000", "猪猪银行"], ["600001", "建设银行"]]
        self._validate(ass_data)

    def _insert_null_data(self):
        data = [["000002", "xxx"], ["000003", None]]
        df = DataFrame(data=data, columns=[CODE, NAME])
        self._operator.insert_data(self._table_name, df)
        assert True  # 预期：不报错

    def _validate(self, ass_data: list) -> None:
        db = self._operator.select_data(self._table_name)
        ass_df = DataFrame(data=ass_data, columns=[CODE, NAME])
        assert self.dataframe_equals(db, ass_df)

    def _select_data(self):
        db = self._operator.select_data("test")
        assert db is None
