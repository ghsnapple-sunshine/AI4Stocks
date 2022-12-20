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
)
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import DC_MINUTE_META
from buffett.common.pendulum import convert_datetime
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.base.slow import SlowHandler
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.list import SseStockListHandler
from buffett.download.handler.tools import select_data_slow
from buffett.download.mysql import Operator
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


class DcMinuteHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super().__init__(
            operator=operator,
            target_list_handler=SseStockListHandler(operator=operator),
            calendar_handler=CalendarHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AK_DC,
            fuquans=[FuquanType.BFQ],
            freq=FreqType.MIN5,
            meta=DC_MINUTE_META,
            field_code=CODE,
            field_name=NAME,
        )

    def _download(self, para: Para) -> DataFrame:
        """
        根据para中指定的条件下载数据

        :param para:            code, fuquan, start, end
        :return:
        """
        # 使用接口（stock_zh_a_hist_min_em，源：东财）
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

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        查询某支股票、某个时间段内的全部数据

        :param para:        code, [start, end]
        :return:
        """
        para = (
            para.clone()
            .with_source(self._source)
            .with_freq(self._freq)
            .with_fuquan(self._fuquans[0])
        )
        return select_data_slow(operator=self._operator, meta=self._META, para=para)
