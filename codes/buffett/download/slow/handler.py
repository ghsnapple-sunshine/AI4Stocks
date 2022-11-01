import logging
from abc import abstractmethod

from pandas import DataFrame

from buffett.common.error import ParamTypeError
from buffett.common.pendelum import DateSpan, Date
from buffett.constants.col import FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from buffett.constants.col.stock import CODE, NAME
from buffett.download.fast.stock_list_handler import StockListHandler as SHandler
from buffett.download.handler import Handler
from buffett.download.mysql import Operator
from buffett.download.para import Para
from buffett.download.slow.recorder import DownloadRecorder as Recorder
from buffett.download.slow.table_name import TableName
from buffett.download.types import FreqType, SourceType, FuquanType


class SlowHandler(Handler, TableName):
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
            raise ParamTypeError('para.span', DateSpan)

        para = para.clone().with_end(Date.today(), para.span.end > Date.today()) \
            .with_source(self._source) \
            .with_freq(self._freq)

        stocks = SHandler(self._operator).get_data()
        records = self._recorder.get_data()
        # 遍历股票清单和复权方式
        tbs = []
        for index, row in stocks.iterrows():
            for fuquan in self._fuquans:
                spara = para.clone().with_code(row[CODE]).with_name(row[NAME]).with_fuquan(fuquan)
                table_name = SlowHandler._get_table_name_by_code(para=spara)
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
        '''
        cur_start_time = DateFactory.create(record[START_DATE].iloc[0])  # datetime类型在DateFrame会被转为Timestamp
        cur_end_time = DateFactory.create(record[END_DATE].iloc[0])
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
        '''
        done_span = DateSpan(record[START_DATE].iloc[0], record[END_DATE].iloc[0])
        todo_span_ls = para.span.subtract(done_span)
        for span in todo_span_ls:
            self._download_and_save_a_stock(para=para.clone().with_span(span=span),
                                            table_name=table_name)
        para.with_span(span=para.span.add(done_span))

    def _download_and_save_a_stock(self,
                                   para: Para,
                                   table_name: str) -> None:
        data = self._download(para=para)
        self._save_to_database(table_name=table_name, df=data)

    @abstractmethod
    def _download(self, para: Para) -> DataFrame:
        pass

    @abstractmethod
    def _save_to_database(self,
                          table_name: str,
                          df: DataFrame) -> None:
        pass

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
