from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common import create_meta, Code
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
from buffett.common.constants.col.stock import NAME, CONCEPT_CODE, CONCEPT_NAME
from buffett.common.pendulum import Date
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.concept.ak_list import ConceptListHandler
from buffett.download.handler.medium import MediumHandler
from buffett.download.handler.tools import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType
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

_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [OPEN, ColType.FLOAT, AddReqType.NONE],
        [CLOSE, ColType.FLOAT, AddReqType.NONE],
        [HIGH, ColType.FLOAT, AddReqType.NONE],
        [LOW, ColType.FLOAT, AddReqType.NONE],
        [CJL, ColType.INT32, AddReqType.NONE],
        [CJE, ColType.FLOAT, AddReqType.NONE],
        [ZF, ColType.FLOAT, AddReqType.NONE],
        [ZDF, ColType.FLOAT, AddReqType.NONE],
        [ZDE, ColType.FLOAT, AddReqType.NONE],
        [HSL, ColType.FLOAT, AddReqType.NONE],
    ]
)


class AkConceptDailyHandler(MediumHandler):
    def __init__(self, operator: Operator):
        super(AkConceptDailyHandler, self).__init__(
            operator=operator,
            list_handler=ConceptListHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AKSHARE_LGLG_PEPB,
            fuquan=FuquanType.BFQ,
            freq=FreqType.DAY,
            field_code=CONCEPT_CODE,
            field_name=CONCEPT_NAME,
        )

    def _download(self, row: tuple) -> DataFrame:
        # 使用接口（stock_board_concept_hist_em，源：东财）,code为Str6
        daily_info = ak.stock_board_concept_hist_em(
            symbol=getattr(row, self._NAME),
            adjust=self._fuquan.ak_format(),
        )
        # 重命名
        daily_info = daily_info.rename(columns=_RENAME)
        # 转换date
        daily_info[DATE] = daily_info[DATE].apply(lambda x: Date.from_str(x))
        return daily_info

    def _save_to_database(self, df: DataFrame, para: Para) -> None:
        if dataframe_not_valid(df):
            return
        table_name = TableNameTool.get_by_code(para=para)
        self._operator.create_table(name=table_name, meta=_META, if_not_exist=True)
        self._operator.insert_data(table_name, df)

    def select_data(self, para: Para) -> Optional[DataFrame]:
        return super(AkConceptDailyHandler, self).select_data(para=para)
