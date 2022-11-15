# 下载沪深股票列表
from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common import Code, create_meta
from buffett.common.tools import dataframe_not_valid
from buffett.constants.col.stock import CODE, NAME
from buffett.constants.table import STK_LS
from buffett.download import Para
from buffett.download.handler.fast.handler import FastHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType

_META = create_meta(meta_list=[
    [CODE, ColType.CODE, AddReqType.KEY],
    [NAME, ColType.CODE, AddReqType.NONE]])


class StockListHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        stocks = ak.stock_info_a_code_name()
        # 剔除三板市场的股票
        stocks = stocks[stocks[CODE].apply(lambda x: (x[0] != '4') & (x[0] != '8'))]
        return stocks

    def _save_to_database(self, df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return

        self._operator.create_table(name=STK_LS, meta=_META)
        self._operator.try_insert_data(name=STK_LS, df=df, update=True, meta=_META)
        self._operator.disconnect()

    def select_data(self,
                    para: Para = None) -> Optional[DataFrame]:
        df = self._operator.select_data(STK_LS)
        if dataframe_not_valid(df):
            return

        df[CODE] = df[CODE].apply(lambda x: Code(x))
        return df
