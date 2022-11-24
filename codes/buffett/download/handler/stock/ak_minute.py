from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common import create_meta
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
)
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.pendulum import convert_datetime
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.list import StockListHandler
from buffett.download.handler.slow.handler import SlowHandler
from buffett.download.handler.tools.table_name import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FuquanType, FreqType

_RENAME = {
    "时间": DATETIME,
    "开盘": OPEN,
    "收盘": CLOSE,
    "最高": HIGH,
    "最低": LOW,
    "成交量": CJL,
    "成交额": CJE,
    "振幅": ZF,
    "涨跌幅": ZDF,
    "涨跌额": ZDE,
    "换手率": HSL,
}

_META = create_meta(
    meta_list=[
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
    ]
)


class AkMinuteHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super().__init__(
            operator=operator,
            target_list_handler=StockListHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AKSHARE_DONGCAI,
            fuquans=[FuquanType.BFQ],
            freq=FreqType.MIN5,
            field_code=CODE,
            field_name=NAME,
        )

    def _download(self, para: Para) -> DataFrame:
        # 使用接口（stock_zh_a_hist_min_em，源：东财）,code为Str6
        minute_info = ak.stock_zh_a_hist_min_em(
            symbol=para.target.code,
            period="5",
            start_date=convert_datetime(para.span.start).format("YYYY-MM-DD HH:mm:ss"),
            end_date=convert_datetime(para.span.end).format("YYYY-MM-DD HH:mm:ss"),
            adjust=para.comb.fuquan.ak_format(),
        )
        if dataframe_not_valid(minute_info):
            return minute_info

        minute_info = minute_info.rename(columns=_RENAME)  # 重命名
        return minute_info

    def _save_to_database(self, table_name: str, df: DataFrame):
        if dataframe_not_valid(df):
            return
        self._operator.create_table(name=table_name, meta=_META)
        self._operator.insert_data(name=table_name, df=df)

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        查询某支股票、某种复权方式下、某个时间段内的全部数据

        :param para:        code, fuquan, start, end
        :return:
        """
        para = para.clone().with_source(self._source).with_freq(self._freq)
        table_name = TableNameTool.get_by_code(para=para)
        df = self._operator.select_data(name=table_name, span=para.span)
        if dataframe_not_valid(df):
            return
        df.index = df[DATETIME]
        del df[DATETIME]
        return df
