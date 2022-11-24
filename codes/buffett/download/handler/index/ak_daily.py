from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common import create_meta
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
from buffett.common.constants.col.target import INDEX_CODE, INDEX_NAME
from buffett.common.pendulum import Date
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.index import AkIndexListHandler
from buffett.download.handler.medium import MediumHandler
from buffett.download.handler.tools import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FuquanType, FreqType

_RENAME = {"volume": CJL, "amount": CJE}

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

    def _save_to_database(self, df: DataFrame, para: Para) -> None:
        if dataframe_not_valid(df):
            return
        table_name = TableNameTool.get_by_code(para=para)
        self._operator.create_table(name=table_name, meta=_META, if_not_exist=True)
        self._operator.insert_data(table_name, df)

    def select_data(self, para: Para) -> Optional[DataFrame]:
        return super(AkIndexDailyHandler, self).select_data(para=para)
