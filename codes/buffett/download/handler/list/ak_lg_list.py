# 下载沪深股票列表
from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common import Code, create_meta
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.table import LG_STK_LS
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.fast.handler import FastHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType

_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
        [NAME, ColType.STOCK_NAME, AddReqType.NONE],
    ]
)

STOCK_NAME = "stock_name"


class AkLgStockListHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        stocks = ak.stock_a_lg_indicator(symbol="all")
        # rename
        stocks = DataFrame({CODE: stocks[CODE], NAME: stocks[STOCK_NAME]})
        # 剔除三板市场的股票
        stocks = stocks[stocks[CODE].apply(lambda x: (x[0] != "4") & (x[0] != "8"))]
        return stocks

    def _save_to_database(self, df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return
        self._operator.create_table(name=LG_STK_LS, meta=_META)
        self._operator.try_insert_data(name=LG_STK_LS, df=df, update=True, meta=_META)

    def select_data(self, para: Para = None) -> Optional[DataFrame]:
        df = self._operator.select_data(LG_STK_LS)
        if dataframe_not_valid(df):
            return
        df[CODE] = df[CODE].apply(lambda x: Code(x))
        return df
