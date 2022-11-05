import pandas as pd
from pandas import DataFrame

from buffett.common import create_meta
from buffett.common.pendelum import Date, DateSpan, DateTime
from buffett.constants.col import DATETIME, OPEN, CLOSE
from buffett.constants.col.stock import CODE
from buffett.constants.dbcol import ROW_NUM
from buffett.download.mysql.types import ColType, AddReqType
from test import Tester


class TestOperatorEx(Tester):
    def setUp(self) -> None:
        super().setUp()
        meta = create_meta(meta_list=[
            [CODE, ColType.STOCK_CODE, AddReqType.KEY],
            [DATETIME, ColType.DATETIME, AddReqType.KEY],
            [OPEN, ColType.FLOAT, AddReqType.NONE],
            [CLOSE, ColType.FLOAT, AddReqType.NONE]])
        self.operator.create_table(name=self.table_name, meta=meta)
        data = [['000001', DateTime(2022, 1, 1), 10.0, 10.5],
                ['000001', DateTime(2022, 1, 2), 10.5, 11.0],
                ['000001', DateTime(2022, 1, 3), 11.0, 10.5],
                ['000001', DateTime(2022, 1, 4), 10.0, 10.5],
                ['000002', DateTime(2022, 1, 1), 10.0, 10.3],
                ['000002', DateTime(2022, 1, 2), 10.3, 9.9],
                ['000002', DateTime(2022, 1, 3), 9.9, 10.1],
                ['000002', DateTime(2022, 1, 4), 10.1, 10.5]]
        self.df = DataFrame(data, columns=[CODE, DATETIME, OPEN, CLOSE])
        self.operator.insert_data(name=self.table_name, df=self.df)

    def test_select(self):
        db = self.operator.select_data(name=self.table_name)
        df = self.df
        cmp = pd.concat([db, df]).drop_duplicates(keep=False)
        assert cmp.empty

    def test_select_row_num(self):
        db_row_num = self.operator.select_row_num(name=self.table_name)
        df_row_num = self.df.shape[0]
        assert df_row_num == db_row_num

    def test_select_with_span(self):
        span = DateSpan(start=Date(2022, 1, 1), end=Date(2022, 1, 4))
        db = self.operator.select_data(name=self.table_name, span=span)
        df = self.df[self.df[DATETIME].apply(lambda x: span.is_in_span(x))]
        cmp = pd.concat([db, df]).drop_duplicates(keep=False)
        assert cmp.empty

    def test_select_row_num_with_group(self):
        db = self.operator.select_row_num(name=self.table_name, groupby=[CODE, DATETIME])
        df = self.df.groupby(by=[CODE, DATETIME]).apply(
            lambda x: DataFrame([[x[CODE].iloc[0], x[DATETIME].iloc[0], x.shape[0]]],
                                columns=[CODE, DATETIME, ROW_NUM]))
        df.reset_index(inplace=True, drop=True)
        cmp = pd.concat([db, df]).drop_duplicates(keep=False)
        assert cmp.empty
