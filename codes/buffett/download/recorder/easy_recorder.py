from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.common import create_meta
from buffett.common.constants.col import START_DATE, END_DATE
from buffett.common.constants.col.task import CLASS, MODULE
from buffett.common.constants.table import EA_RCD
from buffett.common.magic import get_name, get_module_name
from buffett.common.pendulum import DateSpan
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType

_META = create_meta(
    meta_list=[
        [CLASS, ColType.SHORT_DESC, AddReqType.KEY],
        [MODULE, ColType.SHORT_DESC, AddReqType.KEY],
        [START_DATE, ColType.DATETIME, AddReqType.NONE],
        [END_DATE, ColType.DATETIME, AddReqType.NONE],
    ]
)


class EasyRecorder:
    def __init__(self, operator: Operator):
        self._operator = operator
        self._exist = False

    def save(self, cls: type, span: DateSpan) -> None:
        data = DataFrame(
            [[get_name(cls), get_module_name(cls), span.start, span.end]],
            columns=[CLASS, MODULE, START_DATE, END_DATE],
        )
        self.save_to_database(df=data)

    def save_to_database(self, df: DataFrame) -> None:
        if not self._exist:
            self._operator.create_table(EA_RCD, _META, if_not_exist=True)
            self._exist = True

        self._operator.try_insert_data(EA_RCD, df, _META, update=True)  # 如果原纪录已存在，则更新

    def select_data(self, cls: type) -> Optional[DateSpan]:
        df = self._operator.select_data(EA_RCD)
        if dataframe_not_valid(df):
            return
        df = df[(df[CLASS] == get_name(cls)) & (df[MODULE] == get_module_name(cls))]
        if dataframe_not_valid(df):
            return
        series = df.iloc[0]
        return DateSpan(series[START_DATE], series[END_DATE])
