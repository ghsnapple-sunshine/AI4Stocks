from pandas import DataFrame

from buffett.common import Code
from buffett.common.tools import create_meta
from buffett.constants.col import FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from buffett.constants.col.stock import CODE
from buffett.constants.table import DL_RCD
from buffett.download import Para
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType
from buffett.download.types import FreqType, FuquanType, SourceType

_META = create_meta(meta_list=[
    [CODE, ColType.STOCK_CODE_NAME, AddReqType.KEY],
    [FREQ, ColType.ENUM_BOOL, AddReqType.KEY],
    [FUQUAN, ColType.ENUM_BOOL, AddReqType.KEY],
    [SOURCE, ColType.ENUM_BOOL, AddReqType.KEY],
    [START_DATE, ColType.DATETIME, AddReqType.NONE],
    [END_DATE, ColType.DATETIME, AddReqType.NONE]])


class DownloadRecorder:
    def __init__(self, operator: Operator):
        self._operator = operator
        self._exist = False

    def save(self, para: Para):
        ls = [
            [para.stock.code, para.comb.freq, para.comb.fuquan, para.comb.source, para.span.start, para.span.end]
        ]
        cols = [CODE, FREQ, FUQUAN, SOURCE, START_DATE, END_DATE]
        data = DataFrame(data=ls, columns=cols)
        self.save_to_database(data=data)

    def save_to_database(self, data: DataFrame):
        if not self._exist:
            self._operator.create_table(DL_RCD, _META, if_not_exist=True)
            self._exist = True

        self._operator.try_insert_data(DL_RCD, data, _META, update=True)  # 如果原纪录已存在，则更新

    def get_data(self) -> DataFrame:
        df = self._operator.select_data(DL_RCD)
        if (not isinstance(df, DataFrame)) or df.empty:
            return DataFrame()

        df[CODE] = df[CODE].apply(lambda x: Code(x))
        df[FREQ] = df[FREQ].apply(lambda x: FreqType(x))
        df[FUQUAN] = df[FUQUAN].apply(lambda x: FuquanType(x))
        df[SOURCE] = df[SOURCE].apply(lambda x: SourceType(x))
        return df
