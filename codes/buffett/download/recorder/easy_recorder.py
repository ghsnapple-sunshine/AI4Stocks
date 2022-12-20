from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import START_DATE, END_DATE
from buffett.common.constants.col.task import CLASS, MODULE
from buffett.common.constants.meta.recorder import EA_META
from buffett.common.constants.table import EA_RCD
from buffett.common.magic import get_name, get_module_name
from buffett.common.pendulum import DateSpan
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator
from buffett.download.recorder.simple_recorder import SimpleRecorder


class EasyRecorder(SimpleRecorder):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator, table_name=EA_RCD, meta=EA_META)

    def save(self, cls: type, span: DateSpan) -> None:
        data = DataFrame(
            [[get_name(cls), get_module_name(cls), span.start, span.end]],
            columns=[CLASS, MODULE, START_DATE, END_DATE],
        )
        self.save_to_database(df=data)

    def select_data(self, cls: type) -> Optional[DateSpan]:
        df = self._operator.select_data(name=EA_RCD, meta=EA_META)
        if dataframe_not_valid(df):
            return
        df = df[(df[CLASS] == get_name(cls)) & (df[MODULE] == get_module_name(cls))]
        if dataframe_not_valid(df):
            return
        series = df.iloc[0]
        return DateSpan(series[START_DATE], series[END_DATE])
