from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common.common import FuquanType, DataFreqType, DataSourceType
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
                table_name = 'stock_{0}info_{1}_{2}'.format(self.freq, code, fuquan.ToReq())
                if records.shape[0] > 0:  # 存在下载记录
                    record = records[(
                            records['code'] == code and
                            records['freq'] == self.freq and
                            records['fuquan'] == fuquan and
                            records['source'] == self.source
                    )]
                    if record.shape[0] > 0:
                        cur_start_date = record['start_date'].iloc[0, 0]
                        cur_end_date = record['end_date'].iloc[0, 0]
                        if start_date < cur_start_date:
                            self.DownloadAndSaveAStock(code=code, fuquan=fuquan, start_date=start_date,
                                                       end_date=cur_start_date, name=table_name)
                        if end_date > cur_end_date:  # 注意无需elif
                            self.DownloadAndSaveAStock(code=code, fuquan=fuquan, start_date=cur_end_date,
                                                       end_date=end_date, name=table_name)
                    else:
                        self.DownloadAndSaveAStock(code=code, fuquan=fuquan, start_date=start_date,
                                                   end_date=end_date, name=table_name)
                else:
                    self.DownloadAndSaveAStock(code=code, fuquan=fuquan, start_date=start_date,
                                               end_date=end_date, name=table_name)

                print('Successfully Download Stock {0} {1} {2} {3}'.format(
                    self.freq, code.toCode6(), name, fuquan.ToReq()))
                self.recorder.Save(code=code, freq=self.freq, fuquan=fuquan,
                                   source=self.source, start_date=start_date, end_date=end_date)
                tbls.append(table_name)

        return tbls

    def DownloadAndSaveAStock(self, code: StockCode, fuquan: FuquanType, start_date: DateTime, end_date: DateTime,
                              name: str):
        data = self.Download(code=code, fuquan=fuquan, start_date=start_date, end_date=end_date)
        self.Save2Database(name=name, data=data)
