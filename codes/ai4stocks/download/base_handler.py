from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common.type_converter import TypeConverter
from ai4stocks.common.types import FuquanType, DataFreqType, DataSourceType
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.akshare.stock_list_handler import StockListHandler
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.download.download_recorder import DownloadRecorder


class BaseHandler:
    def __init__(self, op: MysqlOperator):
        self.op = op
        self.recorder = DownloadRecorder(op=op)
        self.source = DataSourceType.AKSHARE_DONGCAI
        self.fuquans = [FuquanType.NONE]
        self.freq = DataFreqType.DAY

    def Download(self, code: StockCode, fuquan: FuquanType, start_date: DateTime, end_date: DateTime) -> DataFrame:
        return DataFrame()

    def Save2Database(self, name: str, data: DataFrame) -> None:
        return

    def DownloadAndSave(self, start_date: DateTime, end_date: DateTime) -> list:
        stocks = StockListHandler(self.op).GetTable()
        records = self.recorder.GetTable()
        tbls = []

        for index, row in stocks.iterrows():
            for fuquan in self.fuquans:
                code = row['code']
                name = row['name']
                table_name = self.GetTableName(code=code, fuquan=fuquan)
                record = records[
                    (records['code'] == code) & (records['freq'] == self.freq) &
                    (records['fuquan'] == fuquan) & (records['source'] == self.source)
                    ]
                if record.empty:
                    self.DownloadAndSaveAStock(code=code, fuquan=fuquan, start_date=start_date,
                                               end_date=end_date, name=table_name)
                else:
                    cur_start_date = TypeConverter.Ts2Dt(record['start_date'].iloc[0])
                    cur_end_date = TypeConverter.Ts2Dt(record['end_date'].iloc[0])
                    if start_date < cur_start_date:
                        self.DownloadAndSaveAStock(
                            code=code, fuquan=fuquan, start_date=start_date,
                            end_date=cur_start_date - self.freq.ToDuration(), name=table_name)
                    if end_date > cur_end_date:  # 注意无需elif
                        self.DownloadAndSaveAStock(
                            code=code, fuquan=fuquan, start_date=cur_end_date + self.freq.ToDuration(),
                            end_date=end_date, name=table_name)
                    start_date = start_date if start_date < cur_start_date else cur_start_date
                    end_date = end_date if end_date > cur_end_date else cur_end_date
                    # 也有可能不下载数据

                print('Successfully Download Stock {0} {1} {2} {3}'.format(self.freq, code, name, fuquan))
                self.recorder.Save(code=code, freq=self.freq, fuquan=fuquan, source=self.source,
                                   start_date=start_date, end_date=end_date)
                tbls.append(table_name)

        return tbls

    def DownloadAndSaveAStock(self, code: StockCode, fuquan: FuquanType, start_date: DateTime, end_date: DateTime,
                              name: str):
        if end_date >= start_date:
            data = self.Download(code=code, fuquan=fuquan, start_date=start_date, end_date=end_date)
            self.Save2Database(name=name, data=data)

    def GetTableName(self, code: StockCode, fuquan: FuquanType):
        table_name = '{0}_stock_{1}info_{2}_{3}'.format(self.source.toSql(), self.freq, code, fuquan.ToReq())
        return table_name

    def GetTable(self, code: StockCode, fuquan: FuquanType):
        table_name = self.GetTableName(code=code, fuquan=fuquan)
        return self.op.GetTable(table_name)
