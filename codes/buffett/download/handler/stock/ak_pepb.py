from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common import create_meta
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
from buffett.common.constants.col.stock import CODE
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.list import StockListHandler
from buffett.download.handler.medium import MediumHandler
from buffett.download.handler.tools import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.mysql.types import AddReqType, ColType
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

_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [CODE, ColType.CODE, AddReqType.KEY],
        [SYL, ColType.FLOAT, AddReqType.NONE],
        [DSYL, ColType.FLOAT, AddReqType.NONE],
        [SJL, ColType.FLOAT, AddReqType.NONE],
        [SXL, ColType.FLOAT, AddReqType.NONE],
        [DSXL, ColType.FLOAT, AddReqType.NONE],
        [GXL, ColType.FLOAT, AddReqType.NONE],
        [DGXL, ColType.FLOAT, AddReqType.NONE],
        [ZSZ, ColType.FLOAT, AddReqType.NONE],
    ]
)


class AkStockPePbHandler(MediumHandler):
    def __init__(self, operator: Operator):
        super(AkStockPePbHandler, self).__init__(
            operator=operator,
            list_handler=StockListHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AKSHARE_LGLG_PEPB,
            fuquan=FuquanType.BFQ,
            freq=FreqType.DAY,
        )

    def _download(self, tup: tuple) -> DataFrame:
        code = getattr(tup, CODE)
        pepb = ak.stock_a_lg_indicator(symbol=code)
        # rename
        pepb = pepb.rename(columns=_RENAME)
        pepb[CODE] = code
        return pepb

    def _save_to_database(self, df: DataFrame, para: Para) -> None:
        if dataframe_not_valid(df):
            return
        table_name = TableNameTool.get_by_code(para=para)
        self._operator.create_table(name=table_name, meta=_META)
        self._operator.insert_data(name=table_name, df=df)

    def select_data(self, para: Para) -> Optional[DataFrame]:
        return super(AkStockPePbHandler, self).select_data(para=para)
