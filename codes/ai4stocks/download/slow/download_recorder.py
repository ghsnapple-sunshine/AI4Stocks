from pandas import DataFrame

from ai4stocks.common import Code
from ai4stocks.common.pendelum.tools import timestamp_to_datetime
from ai4stocks.common.tools import create_meta
from ai4stocks.constants.col import FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from ai4stocks.constants.col.stock import CODE
from ai4stocks.constants.table import DL_RCD
from ai4stocks.download import Para
from ai4stocks.download.mysql import AddReqType, ColType, Operator
from ai4stocks.download.types import FreqType, FuquanType, SourceType

_META = create_meta(meta_list=[
    [CODE, ColType.STOCK_CODE, AddReqType.KEY],
    [FREQ, ColType.ENUM, AddReqType.UNSIGNED_KEY],
    [FUQUAN, ColType.ENUM, AddReqType.UNSIGNED_KEY],
    [SOURCE, ColType.ENUM, AddReqType.UNSIGNED_KEY],
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
        df = self._operator.get_table(DL_RCD)
        if (not isinstance(df, DataFrame)) or df.empty:
            return DataFrame()

        df[CODE] = df[CODE].apply(lambda x: Code(x))
        df[FREQ] = df[FREQ].apply(lambda x: FreqType(x))
        df[FUQUAN] = df[FUQUAN].apply(lambda x: FuquanType(x))
        df[SOURCE] = df[SOURCE].apply(lambda x: SourceType(x))
        # df[START_DATE] = df[START_DATE].apply(lambda x: timestamp_to_datetime(x))
        # df[END_DATE] = df[END_DATE].apply(lambda x: timestamp_to_datetime(x))
        return df
