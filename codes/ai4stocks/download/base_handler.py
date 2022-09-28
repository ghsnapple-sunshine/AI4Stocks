from pandas import DataFrame
from pendulum import DateTime, Duration

from ai4stocks.common.types import FuquanType, DataFreqType, DataSourceType
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.akshare.stock_list_handler import StockListHandler
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.download.download_recorder import DownloadRecorder
from ai4stocks.tools.tools import Timestamp2Datetime


def __HandleDateTime__(
        start_time: DateTime,
        end_time: DateTime
) -> tuple:
    ntz_start_time = __SimplifyDateTime__(time=start_time)
    ntz_end_time = __SimplifyDateTime__(time=end_time)
    ntz_last = __SimplifyDateTime__(
        time=DateTime.now(),
        to_day=True
    ) - Duration(minutes=1)
    ntz_end_time = ntz_end_time if ntz_end_time < ntz_last else ntz_last
    return ntz_start_time, ntz_end_time


def __SimplifyDateTime__(
        time: DateTime,
        to_day: bool = False
):
    if to_day:
        return DateTime(year=time.year, month=time.month, day=time.day)
    return DateTime(year=time.year, month=time.month, day=time.day, hour=time.hour, minute=time.minute)


class BaseHandler:
    def __init__(
            self,
            op: MysqlOperator
    ):
        self.op = op
        self.recorder = DownloadRecorder(op=op)
        self.source = DataSourceType.AKSHARE_DONGCAI
        self.fuquans = [FuquanType.NONE]
        self.freq = DataFreqType.DAY

    def __Download__(
            self,
            code: StockCode,
            fuquan: FuquanType,
            start_time: DateTime,
            end_time: DateTime
    ) -> DataFrame:
        return DataFrame()

    def __Save2Database__(
            self,
            name: str,
            data: DataFrame
    ) -> None:
        return

    def DownloadAndSave(
            self,
            start_time: DateTime,
            end_time: DateTime
    ) -> list:
        # 处理start_time和end_time
        start_time, end_time = __HandleDateTime__(
            start_time=start_time,
            end_time=end_time
        )
        stocks = StockListHandler(self.op).GetTable()
        records = self.recorder.GetTable()
        # 遍历股票清单和复权方式
        tbs = []
        for index, row in stocks.iterrows():
            for fuquan in self.fuquans:
                code = row['code']
                name = row['name']
                table_name = self.__GetTableName__(
                    code=code,
                    fuquan=fuquan)
                record = records[
                    (records['code'] == code) &
                    (records['freq'] == self.freq) &
                    (records['fuquan'] == fuquan) &
                    (records['source'] == self.source)]
                if record.empty:
                    self.__DownloadAndSaveAStock__(
                        code=code,
                        fuquan=fuquan,
                        start_time=start_time,
                        end_time=end_time,
                        name=table_name)
                else:
                    start_time, end_time = self.__DownloadPartialInfo__(
                        code=code,
                        fuquan=fuquan,
                        record=record,
                        start_time=start_time,
                        end_time=end_time,
                        table_name=table_name)
                    # 也有可能不下载数据

                print('Successfully Download Stock {0} {1} {2} {3}'.format(
                    self.freq,
                    code,
                    name,
                    fuquan))
                self.recorder.Save(
                    code=code,
                    freq=self.freq,
                    fuquan=fuquan,
                    source=self.source,
                    start_time=start_time,
                    end_time=end_time)
                tbs.append(table_name)

        return tbs

    def __DownloadPartialInfo__(
            self,
            code: StockCode,
            fuquan: FuquanType,
            record: DataFrame,
            start_time: DateTime,
            end_time: DateTime,
            table_name: str
    ) -> tuple:
        cur_start_time = Timestamp2Datetime(record['start_date'].iloc[0])
        cur_end_time = Timestamp2Datetime(record['end_date'].iloc[0])
        if start_time < cur_start_time:
            self.__DownloadAndSaveAStock__(
                code=code,
                fuquan=fuquan,
                start_time=start_time,
                end_time=cur_start_time - self.freq.ToDuration(),
                name=table_name
            )
        if end_time > cur_end_time:  # 注意无需elif
            self.__DownloadAndSaveAStock__(
                code=code,
                fuquan=fuquan,
                start_time=cur_end_time + self.freq.ToDuration(),
                end_time=end_time,
                name=table_name
            )
        start_time = start_time if start_time < cur_start_time else cur_start_time
        end_time = end_time if end_time > cur_end_time else cur_end_time
        return start_time, end_time

    def __DownloadAndSaveAStock__(
            self,
            code: StockCode,
            fuquan: FuquanType,
            start_time: DateTime,
            end_time: DateTime,
            name: str
    ) -> None:
        if end_time >= start_time:
            data = self.__Download__(
                code=code,
                fuquan=fuquan,
                start_time=start_time,
                end_time=end_time)
            self.__Save2Database__(
                name=name,
                data=data)

    def __GetTableName__(
            self,
            code: StockCode,
            fuquan: FuquanType
    ) -> str:
        table_name = '{0}_stock_{1}info_{2}_{3}'.format(
            self.source.toSql(),
            self.freq,
            code,
            fuquan.ToReq())
        return table_name

    def GetTable(
            self,
            code: StockCode,
            fuquan: FuquanType
    ) -> DataFrame:
        table_name = self.__GetTableName__(
            code=code,
            fuquan=fuquan)
        return self.op.GetTable(table_name)
