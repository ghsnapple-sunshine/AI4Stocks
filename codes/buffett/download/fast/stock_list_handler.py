# 下载沪深股票列表
import akshare as ak
from pandas import DataFrame

from buffett.common import Code, create_meta
from buffett.constants.col.stock import CODE, NAME
from buffett.constants.table import STK_LS
from buffett.download import Para
from buffett.download.fast.handler import FastHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType

_META = create_meta(meta_list=[
    [CODE, ColType.STOCK_CODE, AddReqType.KEY],
    [NAME, ColType.STOCK_NAME, AddReqType.NONE]])


class StockListHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        stocks = ak.stock_info_a_code_name()
        # 剔除三板市场的股票
        stocks = stocks[stocks[CODE].apply(lambda x: (x[0] != '4') & (x[0] != '8'))]
        return stocks

    def _save_to_database(self, df: DataFrame) -> None:
        if (not isinstance(df, DataFrame)) or df.empty:
            return

        self._operator.create_table(name=STK_LS, meta=_META)
        self._operator.try_insert_data(name=STK_LS, df=df)  # 忽略重复Insert
        self._operator.disconnect()

    def select_data(self, para: Para = None) -> DataFrame:
        df = self._operator.select_data(STK_LS)
        if (not isinstance(df, DataFrame)) or df.empty:
            return DataFrame()

        df[CODE] = df[CODE].apply(lambda x: Code(x))
        return df
