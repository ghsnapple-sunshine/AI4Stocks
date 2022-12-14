from typing import Optional

from buffett.adapter.baostock import bs
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import (
    OPEN,
    CLOSE,
    HIGH,
    LOW,
    CJL,
    CJE,
    ZDF,
    HSL,
    PRECLOSE,
    ST,
    TP,
)
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import BS_DAILY_META
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.base.slow import SlowHandler
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.list import BsStockListHandler
from buffett.download.handler.tools import select_data_slow
from buffett.download.handler.tools.bs_convert import (
    bs_convert_code,
    bs_check_float,
    bs_check_int,
)
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import FuquanType, SourceType, FreqType

_RENAME = {
    "volume": CJL,
    "amount": CJE,
    "turn": HSL,
    "pctChg": ZDF,
    "isST": ST,
    "tradestatus": TP,
}


class BsDailyHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super().__init__(
            operator=operator,
            target_list_handler=BsStockListHandler(operator=operator),
            calendar_handler=CalendarHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.BS,
            # fuquans=[FuquanType.BFQ, FuquanType.HFQ, FuquanType.QFQ],
            fuquans=[FuquanType.BFQ, FuquanType.HFQ],
            freq=FreqType.DAY,
            meta=BS_DAILY_META,
            field_code=CODE,
            field_name=NAME,
        )

    def _download(self, para: Para) -> Optional[DataFrame]:
        fields = "date,open,high,low,close,preclose,volume,amount,turn,pctChg,tradestatus,isST"
        daily_info = bs.query_history_k_data_plus(
            code=bs_convert_code(para.target.code),
            fields=fields,
            frequency="d",
            start_date=para.span.start.format("YYYY-MM-DD"),
            end_date=para.span.end.subtract(days=1).format("YYYY-MM-DD"),
            adjustflag=para.comb.fuquan.bs_format(),
        )
        if dataframe_not_valid(daily_info):
            return

        # ?????????
        daily_info = daily_info.rename(columns=_RENAME)

        # ????????????
        daily_info.loc[
            :, [OPEN, CLOSE, PRECLOSE, HIGH, LOW, HSL, ZDF]
        ] = daily_info.loc[:, [OPEN, CLOSE, PRECLOSE, HIGH, LOW, HSL, ZDF]].applymap(
            lambda x: bs_check_float(x)
        )
        daily_info.loc[:, [CJL, CJE]] = daily_info.loc[:, [CJL, CJE]].applymap(
            lambda x: bs_check_int(x)
        )

        return daily_info

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        ??????????????????????????????????????????????????????????????????????????????

        :param para:        code, fuquan, [start, end]
        :return:
        """
        para = para.clone().with_freq(self._freq).with_source(self._source)
        return select_data_slow(operator=self._operator, meta=self._META, para=para)
