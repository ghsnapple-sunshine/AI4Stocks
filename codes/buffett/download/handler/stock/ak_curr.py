from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import pd, DataFrame
from buffett.common import create_meta, Code
from buffett.common.constants.col import (
    DATETIME,
    OPEN,
    CLOSE,
    HIGH,
    LOW,
    CJL,
    CJE,
    ZF,
    ZDF,
    ZDE,
    HSL,
    LB,
    DSYL,
    SJL,
    ZSZ,
    LTSZ,
)
from buffett.common.constants.col.target import CODE
from buffett.common.constants.table import STK_RT
from buffett.common.pendulum import DateTime
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.fast.handler import FastHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType

_RENAME = {
    "代码": CODE,
    "今开": OPEN,
    "最新价": CLOSE,
    "最高": HIGH,
    "最低": LOW,
    "成交量": CJL,
    "成交额": CJE,
    "振幅": ZF,
    "涨跌幅": ZDF,
    "涨跌额": ZDE,
    "换手率": HSL,
    "量比": LB,
    "市盈率-动态": DSYL,
    "市净率": SJL,
    "总市值": ZSZ,
    "流通市值": LTSZ,
}

_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
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
        [HSL, ColType.FLOAT, AddReqType.NONE],
        [LB, ColType.FLOAT, AddReqType.NONE],
        [DSYL, ColType.FLOAT, AddReqType.NONE],
        [SJL, ColType.FLOAT, AddReqType.NONE],
        [ZSZ, ColType.FLOAT, AddReqType.NONE],
        [LTSZ, ColType.FLOAT, AddReqType.NONE],
    ]
)


class AkCurrHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator)

    # 接口：stock_zh_a_spot_em
    # 此处__download__不需要参数
    def _download(self) -> DataFrame:
        curr_info = ak.stock_zh_a_spot_em()
        now = DateTime.now()
        curr_info = curr_info.rename(columns=_RENAME)
        curr_info = curr_info[list(_RENAME.values())]
        curr_info[DATETIME] = now
        return curr_info

    def _save_to_database(self, df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return

        self._operator.create_table(name=STK_RT, meta=_META)
        self._operator.try_insert_data(name=STK_RT, df=df)
        self._operator.disconnect()

    def select_data(self, para: Para = None) -> Optional[DataFrame]:
        df = self._operator.select_data(STK_RT)
        if dataframe_not_valid(df):
            return

        df[CODE] = df[CODE].apply(lambda x: Code(x))
        return df
