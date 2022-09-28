from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common.types import DataFreqType, FuquanType, DataSourceType
from ai4stocks.common.constants import DOWN_RECORD_TABLE, META_COLS
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.connect.mysql_common import MysqlColAddReq, MysqlColType
from ai4stocks.download.connect.mysql_operator import MysqlOperator


class DownloadRecorder:
    def __init__(self, op: MysqlOperator):
        self.exist = False
        self.op = op
        cols = [
            ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
            ['freq', MysqlColType.ENUM, MysqlColAddReq.UNSIGNED_KEY],
            ['fuquan', MysqlColType.ENUM, MysqlColAddReq.UNSIGNED_KEY],
            ['source', MysqlColType.ENUM, MysqlColAddReq.UNSIGNED_KEY],
            ['start_date', MysqlColType.DATETIME, MysqlColAddReq.NONE],
            ['end_date', MysqlColType.DATETIME, MysqlColAddReq.NONE]
        ]
        self.col_meta = DataFrame(data=cols, columns=META_COLS)

    def Save(self, code: str, freq: DataFreqType, fuquan: FuquanType, source: DataSourceType,
             start_time: DateTime, end_time: DateTime):
        ls = [[code, freq, fuquan, source, start_time, end_time]]
        cols = ['code', 'freq', 'fuquan', 'source', 'start_date', 'end_date']
        df = DataFrame(data=ls, columns=cols)
        self.Save2Database(data=df)

    def Save2Database(self, data: DataFrame):
        table_name = DOWN_RECORD_TABLE

        if not self.exist:
            self.op.CreateTable(table_name, self.col_meta, if_not_exist=True)
            self.exist = True

        self.op.TryInsertData(table_name, data, self.col_meta, update=True)  # 如果原纪录已存在，则更新

    def GetTable(self):
        table_name = DOWN_RECORD_TABLE
        df = self.op.GetTable(table_name)
        if df.empty:
            return DataFrame(columns=['code', 'freq', 'fuquan', 'source', 'start_date', 'end_date'])
        df['code'] = df.apply(lambda x: StockCode(x['code']), axis=1)
        df['freq'] = df.apply(lambda x: DataFreqType(x['freq']), axis=1)
        df['fuquan'] = df.apply(lambda x: FuquanType(x['fuquan']), axis=1)
        df['source'] = df.apply(lambda x: DataSourceType(x['source']), axis=1)
        return df
