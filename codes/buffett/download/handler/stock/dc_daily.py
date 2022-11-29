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
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import AK_DAILY_META
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.base.slow import SlowHandler
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.list import SseStockListHandler
from buffett.download.handler.tools.table_name import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import FuquanType, SourceType, FreqType

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


class DcDailyHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super().__init__(
            operator=operator,
            target_list_handler=SseStockListHandler(operator=operator),
            calendar_handler=CalendarHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AKSHARE_DONGCAI,
            fuquans=[FuquanType.BFQ, FuquanType.HFQ, FuquanType.QFQ],
            freq=FreqType.DAY,
            field_code=CODE,
            field_name=NAME,
        )

    def _download(self, para: Para) -> DataFrame:
        # 使用接口（stock_zh_a_hist，源：东财）,code为Str6
        # 备用接口（stock_zh_a_daily，源：新浪，未实现）
        daily_info = ak.stock_zh_a_hist(
            symbol=para.target.code,
            period="daily",
            start_date=para.span.start.format("YYYYMMDD"),
            end_date=para.span.end.subtract(days=1).format("YYYYMMDD"),
            adjust=para.comb.fuquan.ak_format(),
        )

        # 重命名
        daily_info = daily_info.rename(columns=_RENAME)
        return daily_info

    def _save_to_database(self, table_name: str, df: DataFrame) -> None:
        self._operator.create_table(
            name=table_name, meta=AK_DAILY_META, if_not_exist=True
        )
        self._operator.insert_data(table_name, df)

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        查询某支股票以某种复权方式的全部数据

        :param para:        code, fuquan
        :return:
        """
        para = para.clone().with_freq(self._freq).with_source(self._source)
        table_name = TableNameTool.get_by_code(para=para)
        df = self._operator.select_data(name=table_name, span=para.span)
        if dataframe_not_valid(df):
            return
        df.index = df[DATE]
        del df[DATE]
        return df
