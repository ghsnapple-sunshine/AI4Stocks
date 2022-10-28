# 下载沪深股票列表
import akshare as ak
from pandas import DataFrame

from ai4stocks.common import Code, create_meta
from ai4stocks.constants.col.stock import CODE, NAME
from ai4stocks.constants.table import STK_LS
from ai4stocks.download import Para
from ai4stocks.download.fast.handler import FastHandler
from ai4stocks.download.mysql import Operator, ColType, AddReqType

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
        self._operator.try_insert_data(name=STK_LS, data=df)  # 忽略重复Insert
        self._operator.disconnect()

    def get_data(self, para: Para = None) -> DataFrame:
        df = self._operator.get_table(STK_LS)
        if (not isinstance(df, DataFrame)) or df.empty:
            return DataFrame()

        df[CODE] = df[CODE].apply(lambda x: Code(x))
        return df
