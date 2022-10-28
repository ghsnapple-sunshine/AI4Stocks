import logging
from abc import abstractmethod

from pandas import DataFrame

from ai4stocks.common.pendelum import DateSpan, Date
from ai4stocks.common.pendelum.tools import timestamp_to_datetime
from ai4stocks.constants.col import FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from ai4stocks.constants.col.stock import CODE, NAME
from ai4stocks.download.fast.stock_list_handler import StockListHandler as SHandler
from ai4stocks.download.handler import Handler
from ai4stocks.download.mysql import Operator
from ai4stocks.download.para import Para
from ai4stocks.download.slow.download_recorder import DownloadRecorder as Recorder
from ai4stocks.download.types import FreqType, SourceType, FuquanType


class SlowHandler(Handler):
    """
    实现多张表的下载，存储
    """

    def __init__(self, operator: Operator):
        super(SlowHandler, self).__init__(operator=operator)
        self._recorder = Recorder(operator=operator)
        self._source = SourceType.AKSHARE_DONGCAI
        self._fuquans = [FuquanType.BFQ]
        self._freq = FreqType.DAY

    # region 公共方法
    def obtain_data(self, para: Para):
        if not isinstance(para.span, DateSpan):
            raise ValueError('An invalid datespan received.')

        para.with_end(Date.yesterday(), para.span.end > Date.yesterday()) \
            .with_source(self._source) \
            .with_freq(self._freq)

        stocks = SHandler(self._operator).get_data()
        records = self._recorder.get_data()
        # 遍历股票清单和复权方式
        tbs = []
        for index, row in stocks.iterrows():
            for fuquan in self._fuquans:
                spara = para.clone().with_code(row[CODE]).with_name(row[NAME]).with_fuquan(fuquan)
                table_name = SlowHandler._get_table_name(para=spara)
                record = DataFrame() if records.empty else SlowHandler._filter_record(
                    para=spara, records=records)
                if record.empty:
                    self._download_and_save_a_stock(para=spara,
                                                    table_name=table_name)
                else:
                    self._download_partial(para=spara,
                                           table_name=table_name,
                                           record=record)
                # 也有可能不下载数据

                self._recorder.save(para=spara)
                tbs.append(table_name)
                self._log_success_download(para=spara)
        return tbs

    @abstractmethod
    def get_data(self, para: Para) -> DataFrame:
        pass

    # endregion

    # region 私有方法

    def _download_partial(self,
                          para: Para,
                          record: DataFrame,
                          table_name: str) -> None:
        cur_start_time = timestamp_to_datetime(record[START_DATE].iloc[0])  # datetime类型在DateFrame会被转为Timestamp
        cur_end_time = timestamp_to_datetime(record[END_DATE].iloc[0])
        if para.span.start < cur_start_time:
            spara = para.clone().with_end(cur_start_time - self._freq.to_duration())
            self._download_and_save_a_stock(para=spara, table_name=table_name)
        else:
            para.with_start(start=cur_start_time)

        if para.span.end > cur_end_time:  # 注意无需elif
            spara = para.clone().with_start(cur_end_time + self._freq.to_duration())
            self._download_and_save_a_stock(para=spara, table_name=table_name)
        else:
            para.with_end(end=cur_end_time)

    def _download_and_save_a_stock(self,
                                   para: Para,
                                   table_name: str) -> None:
        data = self._download(para=para)
        self._save_to_database(name=table_name, data=data)

    @abstractmethod
    def _download(self, para: Para) -> DataFrame:
        pass

    @abstractmethod
    def _save_to_database(self,
                          name: str,
                          data: DataFrame) -> None:
        pass

    @classmethod
    def _get_table_name(cls, para: Para) -> str:
        table_name = '{0}_stock_{1}info_{2}_{3}'.format(para.comb.source.sql_format(),
                                                        para.comb.freq,
                                                        para.stock.code,
                                                        para.comb.fuquan.ak_format())
        return table_name

    @classmethod
    def _log_success_download(cls, para: Para):
        logging.info('Successfully Download Stock {0} {1} {2} {3}'.format(
            para.comb.freq,
            para.stock.code,
            para.stock.name,
            para.comb.fuquan))

    @classmethod
    def _filter_record(cls, para: Para, records: DataFrame) -> DataFrame:
        record = records[(records[CODE] == para.stock.code) &
                         (records[FREQ] == para.comb.freq) &
                         (records[FUQUAN] == para.comb.fuquan) &
                         (records[SOURCE] == para.comb.source)]
        return record
# endregion
