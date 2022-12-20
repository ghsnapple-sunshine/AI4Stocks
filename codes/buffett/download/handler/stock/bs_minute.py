from typing import Optional

from buffett.adapter.baostock import bs
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import DATETIME, OPEN, CLOSE, HIGH, LOW, CJL, CJE
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import BS_MINUTE_META
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.base import SlowHandler
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.list import BsStockListHandler
from buffett.download.handler.tools import (
    bs_str_to_datetime,
    bs_check_float,
    bs_check_int,
    select_data_slow,
)
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FuquanType, FreqType

_RENAME = {"time": DATETIME, "volume": CJL, "amount": CJE}


class BsMinuteHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super().__init__(
            operator=operator,
            target_list_handler=BsStockListHandler(operator=operator),
            calendar_handler=CalendarHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.BS,
            fuquans=[FuquanType.BFQ],
            freq=FreqType.MIN5,
            meta=BS_MINUTE_META,
            field_code=CODE,
            field_name=NAME,
        )

    def _download(self, para: Para) -> DataFrame:
        fields = "time,open,high,low,close,volume,amount"
        minute_info = bs.query_history_k_data_plus(
            code=self._convert_code(para.target.code),
            fields=fields,
            frequency="5",
            start_date=para.span.start.format("YYYY-MM-DD"),
            end_date=para.span.end.format("YYYY-MM-DD"),
            adjustflag=para.comb.fuquan.bs_format(),
        )
        if dataframe_not_valid(minute_info):
            return minute_info

        # 重命名
        minute_info = minute_info.rename(columns=_RENAME)
        # 按照start_date和end_date过滤数据
        minute_info[DATETIME] = minute_info[DATETIME].apply(
            lambda x: bs_str_to_datetime(x)
        )
        minute_info = minute_info[
            minute_info[DATETIME].apply(lambda x: para.span.is_inside(x))
        ]

        # 更改类型
        minute_info.loc[:, [OPEN, CLOSE, HIGH, LOW]] = minute_info.loc[
            :, [OPEN, CLOSE, HIGH, LOW]
        ].applymap(lambda x: bs_check_float(x))
        minute_info.loc[:, [CJL, CJE]] = minute_info.loc[:, [CJL, CJE]].applymap(
            lambda x: bs_check_int(x)
        )

        return minute_info

    @staticmethod
    def _convert_code(code: str) -> str:
        if code[0] == "6":
            return "sh." + code
        elif code[0] == "0":
            return "sz." + code
        elif code[0] == "3":
            return "sz." + code
        raise NotImplemented

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        查询某支股票、某个时间段内的全部数据

        :param para:            code, [start, end]
        :return:
        """
        para = (
            para.clone()
            .with_source(self._source)
            .with_freq(self._freq)
            .with_fuquan(self._fuquans[0])
        )
        return select_data_slow(operator=self._operator, para=para)
