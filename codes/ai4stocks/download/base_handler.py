from pandas import DataFrame
from pendulum import DateTime, Duration

from ai4stocks.common.types import FuquanType, DataFreqType, DataSourceType
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.akshare.stock_list_handler import StockListHandler
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.download.download_recorder import DownloadRecorder
from ai4stocks.common.tools import timestamp_to_datetime


def __handle_date_time__(
        start_time: DateTime,
        end_time: DateTime
) -> tuple:
    ntz_start_time = __simplify_date_time__(time=start_time)
    ntz_end_time = __simplify_date_time__(time=end_time)
    ntz_last = __simplify_date_time__(
        time=DateTime.now(),
        to_day=True
    ) - Duration(minutes=1)
    ntz_end_time = ntz_end_time if ntz_end_time < ntz_last else ntz_last
    return ntz_start_time, ntz_end_time


def __simplify_date_time__(
        time: DateTime,
        to_day: bool = False
):
    if to_day:
        return DateTime(
            year=time.year,
            month=time.month,
            day=time.day
        )
    return DateTime(
        year=time.year,
        month=time.month,
        day=time.day,
        hour=time.hour,
        minute=time.minute
    )


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

    def __download__(
            self,
            code: StockCode,
            fuquan: FuquanType,
            start_time: DateTime,
            end_time: DateTime
    ) -> DataFrame:
        return DataFrame()

    def __save_to_database__(
            self,
            name: str,
            data: DataFrame
    ) -> None:
        return

    def download_and_save(
            self,
            start_time: DateTime,
            end_time: DateTime
    ) -> list:
        # 处理start_time和end_time
        start_time, end_time = __handle_date_time__(
            start_time=start_time,
            end_time=end_time
        )
        stocks = StockListHandler(self.op).get_table()
        records = self.recorder.get_table()
        # 遍历股票清单和复权方式
        tbs = []
        for index, row in stocks.iterrows():
            for fuquan in self.fuquans:
                code = row['code']
                name = row['name']
                table_name = self.__get_table_name__(
                    code=code,
                    fuquan=fuquan)
                record = records[
                    (records['code'] == code) &
                    (records['freq'] == self.freq) &
                    (records['fuquan'] == fuquan) &
                    (records['source'] == self.source)]
                if record.empty:
                    self.__download_and_save_a_stock__(
                        code=code,
                        fuquan=fuquan,
                        start_time=start_time,
                        end_time=end_time,
                        name=table_name)
                else:
                    start_time, end_time = self.__download_partial_info__(
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
                self.recorder.save(
                    code=code,
                    freq=self.freq,
                    fuquan=fuquan,
                    source=self.source,
                    start_time=start_time,
                    end_time=end_time)
                tbs.append(table_name)

        return tbs

    def __download_partial_info__(
            self,
            code: StockCode,
            fuquan: FuquanType,
            record: DataFrame,
            start_time: DateTime,
            end_time: DateTime,
            table_name: str
    ) -> tuple:
        cur_start_time = timestamp_to_datetime(record['start_date'].iloc[0])
        cur_end_time = timestamp_to_datetime(record['end_date'].iloc[0])
        if start_time < cur_start_time:
            self.__download_and_save_a_stock__(
                code=code,
                fuquan=fuquan,
                start_time=start_time,
                end_time=cur_start_time - self.freq.to_duration(),
                name=table_name
            )
        if end_time > cur_end_time:  # 注意无需elif
            self.__download_and_save_a_stock__(
                code=code,
                fuquan=fuquan,
                start_time=cur_end_time + self.freq.to_duration(),
                end_time=end_time,
                name=table_name
            )
        start_time = start_time if start_time < cur_start_time else cur_start_time
        end_time = end_time if end_time > cur_end_time else cur_end_time
        return start_time, end_time

    def __download_and_save_a_stock__(
            self,
            code: StockCode,
            fuquan: FuquanType,
            start_time: DateTime,
            end_time: DateTime,
            name: str
    ) -> None:
        if end_time >= start_time:
            data = self.__download__(
                code=code,
                fuquan=fuquan,
                start_time=start_time,
                end_time=end_time)
            self.__save_to_database__(
                name=name,
                data=data)

    def __get_table_name__(
            self,
            code: StockCode,
            fuquan: FuquanType
    ) -> str:
        table_name = '{0}_stock_{1}info_{2}_{3}'.format(
            self.source.to_sql(),
            self.freq,
            code,
            fuquan.to_req())
        return table_name

    def get_table(
            self,
            code: StockCode,
            fuquan: FuquanType
    ) -> DataFrame:
        table_name = self.__get_table_name__(
            code=code,
            fuquan=fuquan)
        return self.op.get_table(table_name)
