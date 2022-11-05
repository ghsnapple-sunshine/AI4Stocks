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

        stocks = SHandler(self._operator).select_data()
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
    def select_data(self, para: Para) -> DataFrame:
        pass

    # endregion

    # region 私有方法

    def _download_partial(self,
                          para: Para,
                          record: DataFrame,
                          table_name: str) -> None:
        done_span = DateSpan(record[START_DATE].iloc[0], record[END_DATE].iloc[0])
        todo_span_ls = para.span.subtract(done_span)
        """
        logging.info('Should download {0} for {1} {2} {3}'.format(todo_span_ls,
                                                                  para.comb.freq,
                                                                  para.stock.code,
                                                                  para.comb.fuquan))
        """
        for span in todo_span_ls:
            self._download_and_save_a_stock(para=para.clone().with_span(span=span),
                                            table_name=table_name)
        para.with_span(span=para.span.add(done_span))

    def _download_and_save_a_stock(self,
                                   para: Para,
                                   table_name: str) -> None:
        """
        logging.info('Should download {0} for {1} {2} {3}'.format(para.span,
                                                                  para.comb.freq,
                                                                  para.stock.code,
                                                                  para.comb.fuquan))
        """
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


'''
import logging
from abc import abstractmethod

import pandas as pd
from pandas import DataFrame, Series

from buffett.common.error import ParamTypeError
from buffett.common.pendelum import DateSpan, Date
from buffett.common.tools import dataframe_not_valid
from buffett.constants.col import FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from buffett.constants.col.stock import CODE
from buffett.download.fast.stock_list_handler import StockListHandler as SHandler
from buffett.download.handler import Handler
from buffett.download.mysql import Operator
from buffett.download.para import Para
from buffett.download.slow.recorder import DownloadRecorder as Recorder
from buffett.download.slow.table_name import TableName
from buffett.download.types import FreqType, SourceType, FuquanType

_TDRCD_START, _TDRCD_END = 'todo_record_start', 'todo_record_end'
_DORCD_START, _DORCD_END = 'done_record_start', 'done_record_end'


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

        para = para.clone().with_end(Date.today(), para.span.end > Date.today())

        stock_list = SHandler(self._operator).get_data()
        done_records = self._recorder.get_data()

        todo_records = self._get_todo_records(stock_list=stock_list,
                                              done_records=done_records,
                                              para=para)
        comb_records = SlowHandler._get_comb_records(todo_records=todo_records,
                                                     done_records=done_records)
        tbs = [self._download_n_save_a_stock(row) for index, row in comb_records.iterrows()]
        return tbs

    @abstractmethod
    def get_data(self, para: Para) -> DataFrame:
        pass

    # endregion

    # region 私有方法

    def _get_todo_records(self,
                          stock_list: DataFrame,
                          done_records: DataFrame,
                          para: Para) -> DataFrame:
        """
        计算待下载的记录

        :param stock_list:          股票代码
        :param done_records:        下载记录
        :param para:                span
        :return:                    待下载记录
        """
        fuquan_df = DataFrame(self._fuquans, columns=[FUQUAN])
        todo_records = pd.merge(stock_list[[CODE]], fuquan_df, how='cross')
        todo_records[FREQ] = self._freq
        todo_records[SOURCE] = self._source
        todo_records[START_DATE] = para.span.start
        todo_records[END_DATE] = para.span.end
        todo_records = pd.concat([todo_records, done_records, done_records]).drop_duplicates(keep=False)
        return todo_records

    @classmethod
    def _get_comb_records(cls,
                          todo_records: DataFrame,
                          done_records: DataFrame) -> DataFrame:
        """
        将待下载记录和已下载记录进行拼接

        :param todo_records:
        :param done_records:
        :return:
        """
        if dataframe_not_valid(done_records):
            todo_records[_DORCD_START] = pd.NA
            todo_records[_DORCD_END] = pd.NA
            return todo_records

        done_records.rename(columns={START_DATE: _DORCD_START,
                                     END_DATE: _DORCD_END}, inplace=True)
        todo_records = pd.merge(todo_records, done_records, how='left', on=[CODE, FREQ, SOURCE, FUQUAN])
        return todo_records

    def _download_n_save_a_stock(self,
                                 row: Series) -> None:
        para = Para().with_code(row[CODE]) \
            .with_freq(row[FREQ]) \
            .with_source(row[SOURCE]) \
            .with_fuquan(row[FUQUAN])

        todo_span = DateSpan(row[START_DATE], row[END_DATE])
        done_span = None
        if pd.isna(row[_DORCD_START]):
            data = self._download(para=para.with_span(todo_span))
        else:
            done_span = DateSpan(row[_DORCD_START], row[_DORCD_END])
            todo_ls = todo_span.subtract(done_span)
            data = pd.concat([self._download(para=para.with_span(span)) for span in todo_ls])

        table_name = SlowHandler._get_table_name_by_code(para=para)
        self._save_to_database(table_name=table_name, df=data)
        total_span = todo_span if done_span is None else todo_span.add(done_span)
        self._recorder.save(para=para.with_span(total_span))
        """
        logging.info('Should download {0} for {1} {2} {3}'.format(para.span,
                                                                  para.comb.freq,
                                                                  para.stock.code,
                                                                  para.comb.fuquan))
        """

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

# endregion
'''