from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import (
    DATE,
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
from buffett.common.constants.col.target import CONCEPT_CODE, CONCEPT_NAME
from buffett.common.constants.meta.handler import DC_DAILY_META
from buffett.common.pendulum import Date
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.base import SlowHandler
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.concept.dc_list import DcConceptListHandler
from buffett.download.handler.tools import select_data_slow
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FuquanType, FreqType

_RENAME = {
    "日期": DATE,
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


class DcConceptDailyHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super(DcConceptDailyHandler, self).__init__(
            operator=operator,
            target_list_handler=DcConceptListHandler(operator=operator),
            calendar_handler=CalendarHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AKSHARE_DONGCAI_CONCEPT,
            fuquans=[FuquanType.BFQ],
            freq=FreqType.DAY,
            field_code=CONCEPT_CODE,
            field_name=CONCEPT_NAME,
        )

    def _download(self, para: Para) -> DataFrame:
        # 使用接口（stock_board_concept_n_industry_hist_em，源：东财）,采用code作为symbol
        daily_info = ak.stock_board_concept_n_industry_hist_em(
            symbol=para.target.code,
            period=para.comb.freq.ak_format(),
            start_date=para.span.start.format("YYYYMMDD"),
            end_date=para.span.end.subtract(days=1).format("YYYYMMDD"),
            adjust=para.comb.fuquan.ak_format(),
        )
        if dataframe_not_valid(daily_info):
            return daily_info
        # 重命名
        daily_info = daily_info.rename(columns=_RENAME)
        # 转换date
        daily_info[DATE] = daily_info[DATE].apply(lambda x: Date.from_str(x))
        return daily_info

    def _save_to_database(self, table_name: str, df: DataFrame) -> None:
        """
        将下载的数据存放到数据库

        :param table_name:
        :param df:
        :return:
        """
        self._operator.create_table(name=table_name, meta=DC_DAILY_META)
        self._operator.insert_data(table_name, df)

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        查询某个行业、某个时间段内的全部数据

        :param para:        code, [start, end]
        :return:
        """
        para = (
            para.clone()
            .with_fuquan(self._fuquans[0])
            .with_freq(self._freq)
            .with_source(self._source)
        )
        return select_data_slow(operator=self._operator, para=para)
