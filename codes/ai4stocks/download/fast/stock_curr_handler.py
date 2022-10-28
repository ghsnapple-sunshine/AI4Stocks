import akshare as ak
from pandas import DataFrame

from ai4stocks.common import create_meta
from ai4stocks.common.pendelum.tools import timestamp_to_datetime
from ai4stocks.constants.col import DATETIME, OPEN, CLOSE, HIGH, LOW, CJL, CJE, ZF, ZDF, ZDE, HSL
from ai4stocks.constants.table import STK_RT
from ai4stocks.download import Para
from ai4stocks.download.fast.handler import FastHandler
from ai4stocks.download.mysql import ColType, AddReqType, Operator

_META = create_meta(meta_list=[
    [DATETIME, ColType.DATETIME, AddReqType.KEY],
    [OPEN, ColType.FLOAT, AddReqType.NONE],
    [CLOSE, ColType.FLOAT, AddReqType.NONE],
    [HIGH, ColType.FLOAT, AddReqType.NONE],
    [LOW, ColType.FLOAT, AddReqType.NONE],
    [CJL, ColType.INT32, AddReqType.NONE],
    [CJE, ColType.FLOAT, AddReqType.NONE],
    [ZF, ColType.FLOAT, AddReqType.NONE],
    [ZDF, ColType.FLOAT, AddReqType.NONE],
    [ZDE, ColType.FLOAT, AddReqType.NONE],
    [HSL, ColType.FLOAT, AddReqType.NONE]])


class StockCurrHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator)

    # 接口：stock_zh_a_spot_em
    # 此处__download__不需要参数
    def _download(self) -> DataFrame:
        curr_info = ak.stock_zh_a_spot_em()
        return curr_info

    def _save_to_database(self, df: DataFrame) -> None:
        if (not isinstance(df, DataFrame)) or df.empty:
            return

        self._operator.create_table(name=STK_RT, meta=_META)
        self._operator.try_insert_data(name=STK_RT, data=df)
        self._operator.disconnect()

    def get_data(self, para: Para = None) -> DataFrame:
        df = self._operator.get_table(STK_RT)
        if (not isinstance(df, DataFrame)) or df.empty:
            return DataFrame()

        df[DATETIME] = df[DATETIME].apply(lambda x: timestamp_to_datetime(x))
        return df
