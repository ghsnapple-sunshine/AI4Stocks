from buffett.adapter.pandas import DataFrame
from buffett.common import create_meta
from buffett.common.constants.col import DATETIME, OPEN, CLOSE
from buffett.common.constants.col.mysql import ROW_NUM
from buffett.common.constants.col.target import CODE
from buffett.common.pendulum import Date, DateSpan, DateTime
from buffett.download.mysql.types import ColType, AddReqType
from test import Tester, DbSweeper


class TestSelect(Tester):
    _meta = None
    _df = None

    @classmethod
    def _setup_oncemore(cls):
        DbSweeper.cleanup()
        cls._meta = create_meta(
            meta_list=[
                [CODE, ColType.CODE, AddReqType.KEY],
                [DATETIME, ColType.DATETIME, AddReqType.KEY],
                [OPEN, ColType.FLOAT, AddReqType.NONE],
                [CLOSE, ColType.FLOAT, AddReqType.NONE],
            ]
        )
        cls._operator.create_table(name=cls._table_name, meta=cls._meta)
        data = [
            ["000001", DateTime(2022, 1, 1), 10.0, 10.5],
            ["000001", DateTime(2022, 1, 2), 10.5, 11.0],
            ["000001", DateTime(2022, 1, 3), 11.0, 10.5],
            ["000001", DateTime(2022, 1, 4), 10.0, 10.5],
            ["000002", DateTime(2022, 1, 1), 10.0, 10.3],
            ["000002", DateTime(2022, 1, 2), 10.3, 9.9],
            ["000002", DateTime(2022, 1, 3), 9.9, 10.1],
            ["000002", DateTime(2022, 1, 4), 10.1, 10.5],
        ]
        cls._df = DataFrame(data, columns=[CODE, DATETIME, OPEN, CLOSE])
        cls._operator.insert_data(name=cls._table_name, df=cls._df)

    def _setup_always(self) -> None:
        pass

    def test_select(self):
        db = self._operator.select_data(name=self._table_name, meta=self._meta)
        assert self.compare_dataframe(db, self._df)

    def test_select_row_num(self):
        db_row_num = self._operator.select_row_num(
            name=self._table_name, meta=self._meta
        )
        df_row_num = self._df.shape[0]
        assert df_row_num == db_row_num

    def test_select_with_span(self):
        span = DateSpan(start=Date(2022, 1, 1), end=Date(2022, 1, 4))
        db = self._operator.select_data(
            name=self._table_name, meta=self._meta, span=span
        )
        df = self._df[self._df[DATETIME].apply(lambda x: span.is_inside(x))]
        assert self.compare_dataframe(db, df)

    def test_select_row_num_with_groupby(self):
        db = self._operator.select_row_num(
            name=self._table_name, meta=self._meta, groupby=[CODE, DATETIME]
        )
        df = self._df.groupby(by=[CODE, DATETIME]).apply(
            lambda x: DataFrame(
                {
                    CODE: [x[CODE].iloc[0]],
                    DATETIME: [x[DATETIME].iloc[0]],
                    ROW_NUM: [x.shape[0]],
                }
            )
        )
        df.reset_index(inplace=True, drop=True)
        assert self.compare_dataframe(db, df)

    def test_select_row_num_with_groupby_n_where(self):
        span = DateSpan(start=Date(2022, 1, 1), end=Date(2022, 1, 3))
        db = self._operator.select_row_num(
            name=self._table_name, meta=self._meta, groupby=[CODE], span=span
        )
        df = (
            self._df[self._df[DATETIME].apply(lambda x: span.is_inside(x))]
            .groupby(by=[CODE])
            .apply(
                lambda x: DataFrame({CODE: [x[CODE].iloc[0]], ROW_NUM: [x.shape[0]]})
            )
        )
        df.reset_index(inplace=True, drop=True)
        assert self.compare_dataframe(db, df)
