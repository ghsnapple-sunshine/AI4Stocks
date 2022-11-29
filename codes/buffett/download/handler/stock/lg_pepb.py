from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import (
    DATE,
    SYL,
    DSYL,
    SJL,
    SXL,
    DSXL,
    GXL,
    ZSZ,
    DGXL,
)
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import PEPB_META
from buffett.download.handler.base import MediumHandler
from buffett.download.handler.list import SseStockListHandler
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FuquanType, FreqType

_RENAME = {
    "trade_date": DATE,
    "pe": SYL,
    "pe_ttm": DSYL,
    "pb": SJL,
    "ps": SXL,
    "ps_ttm": DSXL,
    "dv_ratio": GXL,
    "dv_ttm": DGXL,
    "total_mv": ZSZ,
}


class LgPePbHandler(MediumHandler):
    def __init__(self, operator: Operator):
        super(LgPePbHandler, self).__init__(
            operator=operator,
            target_list_handler=SseStockListHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AKSHARE_LGLG_PEPB,
            fuquan=FuquanType.BFQ,
            freq=FreqType.DAY,
            field_code=CODE,
            field_name=NAME,
            meta=PEPB_META,
        )

    def _download(self, row: tuple) -> DataFrame:
        code = getattr(row, CODE)
        pepb = ak.stock_a_lg_indicator(symbol=code)
        # rename
        pepb = pepb.rename(columns=_RENAME)
        return pepb
