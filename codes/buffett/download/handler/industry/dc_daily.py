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
from buffett.common.constants.col.target import (
    INDUSTRY_CODE,
    INDUSTRY_NAME,
)
from buffett.common.constants.meta.handler import AK_DAILY_META
from buffett.common.pendulum import Date
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.base import SlowHandler
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.industry.dc_list import DcIndustryListHandler
from buffett.download.handler.tools import TableNameTool
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


class DcIndustryDailyHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super(DcIndustryDailyHandler, self).__init__(
            operator=operator,
            target_list_handler=DcIndustryListHandler(operator=operator),
            calendar_handler=CalendarHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AKSHARE_DONGCAI_INDUSTRY,
            fuquans=[FuquanType.BFQ],
            freq=FreqType.DAY,
            field_code=INDUSTRY_CODE,
            field_name=INDUSTRY_NAME,
        )

    def _download(self, para: Para) -> DataFrame:
        # 使用接口（stock_board_industry_hist_em，源：东财）,采用name作为symbol
        daily_info = ak.stock_board_industry_hist_em(
            symbol=para.target.code,
            period="日k",  # emmm...
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
        self._operator.create_table(
            name=table_name, meta=AK_DAILY_META, if_not_exist=True
        )
        self._operator.insert_data(table_name, df)

    def select_data(self, para: Para) -> Optional[DataFrame]:
        para = para.clone().with_freq(self._freq).with_source(self._source)
        table_name = TableNameTool.get_by_code(para=para)
        df = self._operator.select_data(table_name)
        if dataframe_not_valid(df):
            return
        df.index = df[DATE]
        del df[DATE]
        return df