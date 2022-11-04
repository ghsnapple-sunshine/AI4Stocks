import logging

import pandas as pd
from pandas import DataFrame, Series
from pymysql import IntegrityError

from buffett.common import create_meta
from buffett.common.pendelum import DateSpan, convert_datetime, DateTime
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.constants import NAN
from buffett.constants.col import FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from buffett.constants.col.stock import CODE
from buffett.download.manage.table_manager import TableManager
from buffett.download.mysql import Operator, ColType, AddReqType
from buffett.download.para import Para
from buffett.download.slow.recorder import DownloadRecorder as DRecorder
from buffett.download.slow.rf_recorder import ReformRecorder as RRecorder
from buffett.download.slow.table_name import TableName
from buffett.download.types import CombType

_ADD_META = create_meta(meta_list=[
    [CODE, ColType.STOCK_CODE, AddReqType.KEY]])

_LS_START, _LS_END = 'ls_start', 'ls_end'
_MONTH_START = 'month_start'
_TDLS_START, _TDLS_END = 'todo_list_start', 'todo_list_end'
_DOLS_START, _DOLS_END = 'done_list_start', 'done_list_end'
_TDRCD_START, _TDRCD_END = 'todo_record_start', 'todo_record_end'


class ReformHandler(TableName):
    """
    实现按Code索引转换到按时间索引
    """

    def __init__(self, operator: Operator):
        self._operator = operator
        self._dl_recorder = DRecorder(operator=operator)
        self._dl_records = DataFrame()
        self._rf_recorder = RRecorder(operator=operator)
        self._meta_cache: dict[CombType, DataFrame] = {}

    # region 公共方法
    def reform_data(self) -> None:
        """
        把按照Code组织的股票数据转换成按照Date组织

        :return:
        """
        dl_records = self._dl_recorder.get_data()
        if dataframe_not_valid(dl_records):
            return

        done_records = self._rf_recorder.get_data()
        todo_records = ReformHandler._get_todo_records(dl_records=dl_records,
                                                       done_records=done_records)
        if dataframe_not_valid(todo_records):
            return

        self._dl_records = dl_records
        todo_list, done_list = ReformHandler._get_todo_list(done_records=done_records,
                                                            todo_records=todo_records)
        self._create_tables(done_list=done_list,
                            todo_list=todo_list)
        comb_list = ReformHandler._get_comb_list(todo_list=todo_list,
                                                 done_list=done_list)
        comb_list.groupby(by=[CODE, FREQ, SOURCE, FUQUAN, START_DATE, END_DATE]).apply(
            lambda x: self._reform_n_save_data(x))

        # endregion

        # region 私有方法

    @classmethod
    def _get_todo_records(cls,
                          dl_records: DataFrame,
                          done_records: DataFrame) -> DataFrame:
        """
        根据记录全集和已转换记录计算待转换记录

        :param dl_records:              记录全集
        :param done_records:            已转换记录
        :return:                        待转换记录
        """
        todo_records = pd.concat([dl_records, done_records, done_records]).drop_duplicates(keep=False)
        return todo_records

    @classmethod
    def _get_todo_list(cls,
                       done_records: DataFrame,
                       todo_records: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        根据待转换记录（包括部分待转换记录）和已转换记录计算待转换列表和已转换列表

        :param done_records:            已转换记录
        :param todo_records:            待转换记录
        :return:                        已下载列表，待下载列表
        """
        todo_list = ReformHandler._split_record(records=todo_records)
        done_list = None

        if dataframe_is_valid(done_records):
            done_list = ReformHandler._split_record(records=done_records)
            todo_list_s = todo_list[[CODE, FREQ, SOURCE, FUQUAN, _LS_START, _LS_END]]
            done_list_s = done_list[[CODE, FREQ, SOURCE, FUQUAN, _LS_START, _LS_END]]
            todo_list_s = pd.concat([todo_list_s, done_list_s, done_list_s]).drop_duplicates(keep=False)
            todo_list = pd.merge(todo_list_s, todo_list, how='left',
                                 on=[CODE, FREQ, SOURCE, FUQUAN, _LS_START, _LS_END])

        return todo_list, done_list

    def _create_tables(self,
                       done_list: DataFrame,
                       todo_list: DataFrame) -> None:
        """
        根据待下载记录和已下载记录，生成Mysql表

        :param done_list:               待转换记录
        :param todo_list:               已转换记录
        :return:                        None
        """
        todo_tables = todo_list[[FREQ, SOURCE, FUQUAN, _LS_START, _LS_END]].drop_duplicates()
        if dataframe_is_valid(done_list):
            done_tables = done_list[[FREQ, SOURCE, FUQUAN, _LS_START, _LS_END]].drop_duplicates()
            todo_tables = pd.concat([todo_tables, done_tables, done_tables]).drop_duplicates(keep=False)

        for index, row in todo_tables.iterrows():
            para = Para().with_freq(row[FREQ]) \
                .with_source(row[SOURCE]) \
                .with_fuquan(row[FUQUAN]) \
                .with_start_n_end(start=row[_LS_START], end=row[_LS_END])
            table_name = ReformHandler._get_table_name_by_date(para=para)
            meta = self._get_meta_cache(para=para)
            self._operator.create_table(name=table_name, meta=meta)

    def _get_meta_cache(self,
                        para: Para) -> DataFrame:
        """
        获取表格元数据

        :param para:            freq, source, fuquan
        :return:
        """
        if para.comb in self._meta_cache.keys():
            meta = self._meta_cache[para.comb]
        else:
            meta = self._create_meta(para=para)
            self._meta_cache[para.comb] = meta
        return meta

    @classmethod
    def _split_record(cls, records: DataFrame) -> DataFrame:
        """
        根据待转换（已转换）的记录计算需要创建（已创建）的Mysql表

        :param records:             记录
        :return:
        """
        spans = records[[START_DATE, END_DATE]].drop_duplicates()
        ls = pd.concat([ReformHandler._create_date_series(row) for index, row in spans.iterrows()])
        ls = pd.merge(records, ls, how='left')
        return ls

    @classmethod
    def _get_comb_list(cls,
                       todo_list: DataFrame,
                       done_list: DataFrame) -> DataFrame:
        """
        将todo_list, todo_record, done_list进行拼接

        :param todo_list:           待转换列表
        :param done_list:           已转换列表
        :return:
        """
        todo_list.rename(columns={_LS_START: _TDLS_START,
                                  _LS_END: _TDLS_END}, inplace=True)
        if dataframe_is_valid(done_list):
            done_list.rename(columns={START_DATE: _DOLS_START,
                                      END_DATE: _DOLS_END}, inplace=True)
            todo_list = pd.merge(todo_list, done_list, how='left',
                                 on=[CODE, FREQ, SOURCE, FUQUAN, _MONTH_START])
        else:
            todo_list[_DOLS_START] = NAN
            todo_list[_DOLS_END] = NAN

        return todo_list

    @classmethod
    def _create_date_series(cls, spans: Series) -> DataFrame:
        """
        获取指定时间范围内的时间分段清单

        :param spans:               时间范围
        :return:                    时间分段清单
        """
        start = convert_datetime(spans[START_DATE])
        end = convert_datetime(spans[END_DATE])
        month_start = DateTime(start.year, start.month, 1)

        dates = []
        while month_start < end:
            month_end = month_start.add(months=1)
            dates.append([start, end, max(month_start, start), min(month_end, end), month_start])
            month_start = month_start.add(months=1)
        return DataFrame(dates, columns=[START_DATE, END_DATE, _LS_START, _LS_END, _MONTH_START])

    def _create_meta(self,
                     para: Para) -> DataFrame:
        """
        按照原表的结构创建Meta

        :param para:                    freq, source, fuquan
        :return:                        meta
        """
        para = para.clone()
        dl_records = self._dl_records
        record = dl_records[(dl_records[FREQ] == para.comb.freq) &
                            (dl_records[SOURCE] == para.comb.source) &
                            (dl_records[FUQUAN] == para.comb.fuquan)].iloc[0, :]
        para.with_code(code=record[CODE])
        table_name = ReformHandler._get_table_name_by_code(para=para)
        meta = TableManager(operator=self._operator).get_meta(name=table_name)
        meta = pd.concat([meta, _ADD_META])
        return meta

    def _reform_n_save_data(self, group: DataFrame) -> None:
        for index, row in group.iterrows():
            self._get_n_reform_data(row)
        df = group[[CODE, FREQ, SOURCE, FUQUAN, START_DATE, END_DATE]].head(1)
        self._rf_recorder.save_to_database(df=df)
        """
        ser = df.iloc[0]
        logging.info('Successfully convert stock {0} {1} {2} ({3}, {4})'.format(
            ser[FREQ], ser[CODE], ser[FUQUAN], ser[START_DATE], ser[END_DATE]))
        """

    def _get_n_reform_data(self, row: Series) -> None:
        """
        按照指定的范围，转换数据

        :param row:                     范围
        :return:
        """
        para = Para().with_code(row[CODE]) \
            .with_freq(row[FREQ]) \
            .with_source(row[SOURCE]) \
            .with_fuquan(row[FUQUAN])
        table_name_by_code = ReformHandler._get_table_name_by_code(para=para)

        todo_dt = DateSpan(start=row[_TDLS_START], end=row[_TDLS_END])
        if pd.isna(row[_DOLS_START]):  # 如果不是部分待完成
            todo_ls = [todo_dt]
        else:
            done_dt = DateSpan(start=row[_DOLS_START], end=row[_DOLS_END])
            todo_ls = todo_dt.subtract(done_dt)
            logging.info('Should convert stock {0} {1} {2} {3}'.format(
                para.comb.fuquan,
                para.stock.code,
                para.comb.fuquan,
                todo_ls))
        for dt in todo_ls:
            para.with_span(span=dt)
            self._get_n_reform_cut(para=para, table_name_by_code=table_name_by_code)

    def _get_n_reform_cut(self,
                          para: Para,
                          table_name_by_code: str):
        """
        按照指定的范围，转换数据

        :param para:
        :param table_name_by_code:
        :return:
        """
        data = self._operator.get_data(name=table_name_by_code, span=para.span)
        if dataframe_not_valid(data):
            return

        data[CODE] = para.stock.code  # 增加code列
        table_name_by_date = ReformHandler._get_table_name_by_date(para=para)

        try:
            self._operator.insert_data(name=table_name_by_date, df=data)
        except IntegrityError as e:
            """
            logging.warning('Direct insert {0} {1} {2} {3} failed, use try insert mode.'.format(
                para.comb.freq,
                para.stock.code,
                para.comb.fuquan,
                para.span))
            """
            logging.warning(e)
            if e.args[0] == 1062:
                meta = self._get_meta_cache(para=para)
                self._operator.try_insert_data(name=table_name_by_date, df=data, meta=meta)
            else:
                raise e
    # endregion
