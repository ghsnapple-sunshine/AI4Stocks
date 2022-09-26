from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common.common import DataFreqType, FuquanType, DataSourceType
from ai4stocks.download.connect.mysql_common import MysqlColAddReq, MysqlColType, MysqlConstants
from ai4stocks.download.connect.mysql_operator import MysqlOperator


class DownloadRecorder:
    def __init__(self, op: MysqlOperator):
        self.exist = False
        self.op = op

    def Save(self, code: str, freq: DataFreqType, fuquan: FuquanType, source: DataSourceType,
             start_date: DateTime, end_date: DateTime):
        ls = [[code, freq.ToReq(), fuquan.ToReq(), source.toSql(), start_date, end_date]]
        cols = ['code', 'freq', 'fuquan', 'source', 'start_date', 'end_date']
        df = DataFrame(data=ls, columns=cols)
        self.Save2Database(data=df)

    def Save2Database(self, data: DataFrame):
        table_name = MysqlConstants.DOWN_RECORD_TABLE

        if not self.exist:
            cols = [
                ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.PRIMKEY],
                ['freq', MysqlColType.DATA_FREQ, MysqlColAddReq.NONE],
                ['fuquan', MysqlColType.DATA_FREQ, MysqlColAddReq.NONE],
                ['source', MysqlColType.DATA_SOURCE, MysqlColAddReq.NONE],
                ['start_date', MysqlColType.DATETIME, MysqlColAddReq.NONE],
                ['end_date', MysqlColType.DATETIME, MysqlColAddReq.NONE]
            ]
            table_meta = DataFrame(data=cols, columns=MysqlConstants.META_COLS)
            self.op.CreateTable(table_name, table_meta, if_not_exist=True)
            self.exist = True

        self.op.TryInsertData(table_name, data, update=True)  # 如果原纪录已存在，则更新
        self.op.Disconnect()

    def GetTable(self):
        table_name = MysqlConstants.DOWN_RECORD_TABLE
        df = self.op.GetTable(table_name)
        if df.empty:
            return DataFrame(columns=['code', 'freq', 'fuquan', 'source', 'start_date', 'end_date'])
        return df
