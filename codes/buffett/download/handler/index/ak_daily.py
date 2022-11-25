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
from buffett.download.handler.index import AkIndexListHandler
from buffett.download.handler.base import MediumHandler
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FuquanType, FreqType

_RENAME = {"volume": CJL, "amount": CJE}


class AkIndexDailyHandler(MediumHandler):
    def __init__(self, operator: Operator):
        super(AkIndexDailyHandler, self).__init__(
            operator=operator,
            target_list_handler=AkIndexListHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AKSHARE_DONGCAI_INDEX,
            fuquan=FuquanType.BFQ,
            freq=FreqType.DAY,
            field_code=INDEX_CODE,
            field_name=INDEX_NAME,
            meta=AK_DAILY_META,
        )

    def _download(self, row: tuple) -> DataFrame:
        # 使用接口（stock_zh_index_daily_em，源：东财）
        code = getattr(row, self._CODE)
        code = "sh" + code if code.startswith("00") else "sz" + code
        daily_info = ak.stock_zh_index_daily_em(symbol=code)
        if dataframe_not_valid(daily_info):
            return daily_info
        # 重命名
        daily_info = daily_info.rename(columns=_RENAME)
        # 转换date
        daily_info[DATE] = daily_info[DATE].apply(lambda x: Date.from_str(x))
        return daily_info
