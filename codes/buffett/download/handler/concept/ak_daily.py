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
from buffett.common.constants.meta.handler import AK_DAILY_META
from buffett.common.pendulum import Date
from buffett.download.handler.concept.ak_list import AkConceptListHandler
from buffett.download.handler.base import MediumHandler
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


class AkConceptDailyHandler(MediumHandler):
    def __init__(self, operator: Operator):
        super(AkConceptDailyHandler, self).__init__(
            operator=operator,
            target_list_handler=AkConceptListHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AKSHARE_DONGCAI_CONCEPT,
            fuquan=FuquanType.BFQ,
            freq=FreqType.DAY,
            field_code=CONCEPT_CODE,
            field_name=CONCEPT_NAME,
            meta=AK_DAILY_META,
        )

    def _download(self, row: tuple) -> DataFrame:
        # 使用接口（stock_board_concept_hist_em，源：东财）,采用name作为symbol
        daily_info = ak.stock_board_concept_hist_em(
            symbol=getattr(row, self._NAME),
            adjust=self._fuquan.ak_format(),
        )
        # 重命名
        daily_info = daily_info.rename(columns=_RENAME)
        # 转换date
        daily_info[DATE] = daily_info[DATE].apply(lambda x: Date.from_str(x))
        return daily_info
