from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import (
    DATE,
    CJL,
    CJE,
)
from buffett.common.constants.col.target import INDEX_CODE, INDEX_NAME
from buffett.common.constants.meta.handler import AK_DAILY_META
from buffett.common.pendulum import Date
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.base import SlowHandler
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.index.dc_list import DcIndexListHandler
from buffett.download.handler.tools import select_data_slow
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FuquanType, FreqType

_RENAME = {"volume": CJL, "amount": CJE}


class DcIndexDailyHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super(DcIndexDailyHandler, self).__init__(
            operator=operator,
            target_list_handler=DcIndexListHandler(operator=operator),
            calendar_handler=CalendarHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AKSHARE_DONGCAI_INDEX,
            fuquans=[FuquanType.BFQ],
            freq=FreqType.DAY,
            field_code=INDEX_CODE,
            field_name=INDEX_NAME,
        )

    def _download(self, para: Para) -> DataFrame:
        """
        根据para中指定的条件下载数据

        :param para:
        :return:
        """
        # 使用接口（stock_zh_index_daily_em，源：东财）
        code = para.target.code
        code = "sh" + code if code.startswith("00") else "sz" + code
        daily_info = ak.stock_zh_index_daily_em(
            symbol=code,
            start_date=para.span.start.format("YYYYMMDD"),
            end_date=para.span.end.subtract(days=1).format("YYYYMMDD"),
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
        self._operator.create_table(name=table_name, meta=AK_DAILY_META)
        self._operator.insert_data(table_name, df)

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        查询某个指数、某个时间段内的全部数据

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
