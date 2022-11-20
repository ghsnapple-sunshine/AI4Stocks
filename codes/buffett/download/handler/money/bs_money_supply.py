from typing import Optional

from buffett.adapter.baostock import bs
from buffett.adapter.pandas import DataFrame
from buffett.common import create_meta
from buffett.common.constants.col import DATE
from buffett.common.constants.col.money import (
    MO,
    M0TB,
    M0HB,
    M1,
    M1TB,
    M1HB,
    M2,
    M2TB,
    M2HB,
)
from buffett.common.constants.table import MONEY_SPLY
from buffett.common.pendulum import Date
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.fast.handler import FastHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType

_RENAME = {
    "m0Month": MO,
    "m0YOY": M0TB,
    "m0ChainRelative": M0HB,
    "m1Month": M1,
    "m1YOY": M1TB,
    "m1ChainRelative": M1HB,
    "m2Month": M2,
    "m2YOY": M2TB,
    "m2ChainRelative": M2HB,
}

_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [MO, ColType.FLOAT, AddReqType.NONE],
        [M0TB, ColType.FLOAT, AddReqType.NONE],
        [M0HB, ColType.FLOAT, AddReqType.NONE],
        [M1, ColType.FLOAT, AddReqType.NONE],
        [M1TB, ColType.FLOAT, AddReqType.NONE],
        [M1HB, ColType.FLOAT, AddReqType.NONE],
        [M2, ColType.FLOAT, AddReqType.NONE],
        [M2TB, ColType.FLOAT, AddReqType.NONE],
        [M2HB, ColType.FLOAT, AddReqType.NONE],
    ]
)

_YEAR = "statYear"
_MONTH = "statMonth"


class BsMoneySupplyHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        bs.login()

        money_supply = bs.query_money_supply_data_month(
            start_date="2000-01", end_date=Date.today().format("YYYY-MM")
        )
        # 重命名
        money_supply = money_supply.rename(columns=_RENAME)
        # 处理日期
        money_supply[DATE] = money_supply.apply(
            lambda row: Date(int(row[_YEAR]), int(row[_MONTH]), 1), axis=1
        )
        del money_supply[_YEAR], money_supply[_MONTH]
        bs.logout()
        return money_supply

    def _save_to_database(self, df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return

        self._operator.create_table(name=MONEY_SPLY, meta=_META)
        self._operator.try_insert_data(name=MONEY_SPLY, df=df)  # 忽略重复Insert
        self._operator.disconnect()

    def select_data(self, para: Para = None) -> Optional[DataFrame]:
        df = self._operator.select_data(MONEY_SPLY)
        if dataframe_not_valid(df):
            return
        return df
