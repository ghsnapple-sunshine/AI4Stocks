from typing import Optional

from buffett.adapter.baostock import bs
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import (
    DATE,
    OPEN,
    CLOSE,
    HIGH,
    LOW,
    CJL,
    CJE,
    ZDF,
    HSL,
)
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import BS_DAILY_META
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.base.slow import SlowHandler
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.list import StockListHandler
from buffett.download.handler.tools.bs_convert import (
    bs_convert_code,
    bs_check_float,
    bs_check_int,
)
from buffett.download.handler.tools.table_name import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import FuquanType, SourceType, FreqType

_RENAME = {"volume": CJL, "amount": CJE, "turn": HSL, "pctChg": ZDF}


class BsDailyHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super().__init__(
            operator=operator,
            target_list_handler=StockListHandler(operator=operator),
            calendar_handler=CalendarHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.BAOSTOCK,
            fuquans=[FuquanType.BFQ, FuquanType.HFQ, FuquanType.QFQ],
            freq=FreqType.DAY,
            field_code=CODE,
            field_name=NAME,
        )

    def _download(self, para: Para) -> Optional[DataFrame]:
        fields = "date,open,high,low,close,volume,amount,turn,pctChg"
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

        # 重命名
        daily_info = daily_info.rename(columns=_RENAME)

        # 更改类型
        daily_info[OPEN] = daily_info[OPEN].apply(lambda x: bs_check_float(x))
        daily_info[CLOSE] = daily_info[CLOSE].apply(lambda x: bs_check_float(x))
        daily_info[HIGH] = daily_info[HIGH].apply(lambda x: bs_check_float(x))
        daily_info[LOW] = daily_info[LOW].apply(lambda x: bs_check_float(x))
        daily_info[CJL] = daily_info[CJL].apply(lambda x: bs_check_int(x))
        daily_info[CJE] = daily_info[CJE].apply(lambda x: bs_check_float(x))
        daily_info[HSL] = daily_info[HSL].apply(lambda x: bs_check_float(x))
        daily_info[ZDF] = daily_info[ZDF].apply(lambda x: bs_check_float(x))

        return daily_info

    def _save_to_database(self, table_name: str, df: DataFrame) -> None:
        self._operator.create_table(name=table_name, meta=BS_DAILY_META)
        self._operator.insert_data(table_name, df)

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        查询某支股票以某种复权方式的全部数据

        :param para:        code, fuquan, [start, end]
        :return:
        """
        para = para.clone().with_freq(self._freq).with_source(self._source)
        table_name = TableNameTool.get_by_code(para=para)
        df = self._operator.select_data(table_name)
        if dataframe_not_valid(df):
            return
        df.index = df[DATE]
        del df[DATE]
        return df