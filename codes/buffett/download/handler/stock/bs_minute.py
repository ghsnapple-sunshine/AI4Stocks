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
from buffett.download.handler.list import StockListHandler
from buffett.download.handler.tools import (
    bs_str_to_datetime,
    bs_check_float,
    bs_check_int,
)
from buffett.download.handler.tools.table_name import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FuquanType, FreqType

_RENAME = {"time": DATETIME, "volume": CJL, "amount": CJE}


class BsMinuteHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super().__init__(
            operator=operator,
            target_list_handler=StockListHandler(operator=operator),
            calendar_handler=CalendarHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.BAOSTOCK,
            fuquans=[FuquanType.BFQ],
            freq=FreqType.MIN5,
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
        minute_info[OPEN] = minute_info[OPEN].apply(lambda x: bs_check_float(x))
        minute_info[CLOSE] = minute_info[CLOSE].apply(lambda x: bs_check_float(x))
        minute_info[HIGH] = minute_info[HIGH].apply(lambda x: bs_check_float(x))
        minute_info[LOW] = minute_info[LOW].apply(lambda x: bs_check_float(x))
        minute_info[CJL] = minute_info[CJL].apply(lambda x: bs_check_int(x))
        minute_info[CJE] = minute_info[CJE].apply(lambda x: bs_check_float(x))

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

    def _save_to_database(self, table_name: str, df: DataFrame) -> None:
        self._operator.create_table(name=table_name, meta=BS_MINUTE_META)
        self._operator.insert_data(table_name, df)

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        查询某支股票以某种复权方式的全部数据

        :param para:        code, fuquan, [start, end]
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
