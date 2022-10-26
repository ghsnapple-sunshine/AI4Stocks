from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common import DOWNLOAD_RECORD_TABLE, META_COLS, StockCode, DataFreqType, FuquanType, DataSourceType
from ai4stocks.download.connect import MysqlColAddReq, MysqlColType, MysqlOperator


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

    def save(
            self,
            code: str,
            freq: DataFreqType,
            fuquan: FuquanType,
            source: DataSourceType,
            start_time: DateTime,
            end_time: DateTime
    ):
        ls = [[code, freq, fuquan, source, start_time, end_time]]
        cols = ['code', 'freq', 'fuquan', 'source', 'start_date', 'end_date']
        data = DataFrame(data=ls, columns=cols)
        self.save_to_database(data=data)

    def save_to_database(self, data: DataFrame):
        table_name = DOWNLOAD_RECORD_TABLE

        if not self.exist:
            self.op.create_table(table_name, self.col_meta, if_not_exist=True)
            self.exist = True

        self.op.try_insert_data(table_name, data, self.col_meta, update=True)  # 如果原纪录已存在，则更新

    def get_table(self):
        table_name = DOWNLOAD_RECORD_TABLE
        df = self.op.get_table(table_name)
        if df.empty:
            return DataFrame(columns=['code', 'freq', 'fuquan', 'source', 'start_date', 'end_date'])
        df['code'] = df.apply(lambda x: StockCode(x['code']), axis=1)
        df['freq'] = df.apply(lambda x: DataFreqType(x['freq']), axis=1)
        df['fuquan'] = df.apply(lambda x: FuquanType(x['fuquan']), axis=1)
        df['source'] = df.apply(lambda x: DataSourceType(x['source']), axis=1)
        return df
