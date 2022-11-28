from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
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
from buffett.common.constants.meta.handler import STK_RT_META
from buffett.common.constants.table import STK_RT
from buffett.common.pendulum import DateTime
from buffett.download.handler.base import FastHandler
from buffett.download.mysql import Operator

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


class DcCurrHandler(FastHandler):
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
        self._operator.create_table(name=STK_RT, meta=STK_RT_META)
        self._operator.try_insert_data(name=STK_RT, df=df)

    def select_data(self) -> Optional[DataFrame]:
        """
        获取实时股票数据

        :return:
        """
        return self._operator.select_data(STK_RT)
