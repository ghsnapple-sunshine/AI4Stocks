import akshare as ak
from pandas import DataFrame

from buffett.common import create_meta
from buffett.common.pendelum.tools import timestamp_to_datetime
from buffett.constants.col import DATETIME, OPEN, CLOSE, HIGH, LOW, CJL, CJE, ZF, ZDF, ZDE, HSL
from buffett.constants.table import STK_RT
from buffett.download import Para
from buffett.download.fast.handler import FastHandler
from buffett.download.mysql import ColType, AddReqType, Operator

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
