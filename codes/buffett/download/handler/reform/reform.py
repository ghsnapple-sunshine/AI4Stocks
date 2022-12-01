from __future__ import annotations

from typing import Optional

from pymysql import IntegrityError

from buffett.adapter import logging
from buffett.adapter.numpy import NAN
from buffett.adapter.pandas import DataFrame, pd
from buffett.common import create_meta
from buffett.common.constants.col import (
    FREQ,
    FUQUAN,
    SOURCE,
    START_DATE,
    END_DATE,
    DATE,
    DATETIME,
)
from buffett.common.constants.col.my import MONTH_START, DORCD_START, DORCD_END
from buffett.common.constants.col.target import CODE
from buffett.common.logger import Logger
from buffett.common.pendulum import DateSpan, DateTime
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid, list_not_valid
from buffett.download.handler.tools.table_name import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType
from buffett.download.para import Para
from buffett.download.recorder.dl_recorder import DownloadRecorder as DRecorder
from buffett.download.recorder.rf_recorder import ReformRecorder as RRecorder
from buffett.download.types import CombType

ADD_META = create_meta(meta_list=[[CODE, ColType.CODE, AddReqType.KEY]])
DATA_THLD = 1_000_000
OBJ_THLD = 30


class ReformHandler:
    """
    实现按Code索引转换到按时间索引
    """

    def __init__(self, operator: Operator):
        self._operator = operator
        self._dl_recorder = DRecorder(operator=operator)
        self._rf_recorder = RRecorder(operator=operator)
        self._reset_datas()

    def _reset_datas(self):
        self._todo_records = DataFrame()
        self._done_records = DataFrame()
        self._meta_cache: dict[CombType, DataFrame] = {}
        self._data_caches: list[ReformObject] = []
        self._data_cache_num = 0

    # region 公共方法
    def reform_data(self) -> None:
        """
        把按照Code组织的股票数据转换成按照Date组织

        :return:
        """
        # 1. 获取待转换记录
        self._get_todo_records()
        if dataframe_not_valid(self._todo_records):
            return
        # 2. 创建表格
        self._create_tables()
        # 3. 拼接待转换记录
        comb_records = self._get_comb_records()
        # 4、 分组转换
        comb_records.groupby(by=[FREQ, SOURCE, FUQUAN]).apply(
            lambda group: self._reform_n_save_data(group)
        )
        # 5、清理现场
        self._reset_datas()

    # endregion

    # region 私有方法

    def _get_todo_records(self) -> None:
        """
        根据记录全集和已转换记录计算待转换记录

        :return:                    待转换记录
        """
        todo_records = self._dl_recorder.select_data()
        if dataframe_not_valid(todo_records):
            return
        done_records = self._rf_recorder.select_data()
        if dataframe_is_valid(done_records):
            self._done_records = done_records
            todo_records = pd.subtract(todo_records, done_records)

        self._todo_records = todo_records

    def _get_comb_records(self) -> DataFrame:
        """
        将todo_record, done_record进行拼接，并分组

        :return:
        """
        todo_records = self._todo_records
        if dataframe_is_valid(self._done_records):
            done_records = self._done_records.rename(
                columns={START_DATE: DORCD_START, END_DATE: DORCD_END}
            )
            todo_records = pd.merge(
                todo_records, done_records, how="left", on=[CODE, FREQ, SOURCE, FUQUAN]
            )
        else:
            todo_records[DORCD_START] = NAN
            todo_records[DORCD_END] = NAN
        return todo_records

    def _create_tables(self) -> None:
        """
        根据待下载记录和已下载记录，生成Mysql表

        :return:                    None
        """
        todo_tables = TableNameTool.get_multi_by_date(self._todo_records)
        if dataframe_is_valid(self._done_records):
            done_tables = TableNameTool.get_multi_by_date(self._done_records)
            todo_tables = pd.subtract(todo_tables, done_tables)

        curr_table_names = self._operator.get_table_list()
        for row in todo_tables.itertuples(index=False):
            para = Para.from_tuple(row).with_start(getattr(row, MONTH_START))
            table_name = TableNameTool.get_by_date(para=para)
            if table_name not in curr_table_names:
                meta = self._get_meta_cache(para=para)
                self._operator.create_table(name=table_name, meta=meta)

    def _get_meta_cache(self, para: Para) -> DataFrame:
        """
        获取表格元数据

        :param para:                freq, source, fuquan
        :return:
        """
        if para.comb in self._meta_cache.keys():
            meta = self._meta_cache[para.comb]
        else:
            meta = self._create_meta(para=para)
            self._meta_cache[para.comb] = meta
        return meta

    def _create_meta(self, para: Para) -> DataFrame:
        """
        按照原表的结构创建Meta

        :param para:                freq, source, fuquan
        :return:                    meta
        """
        para = para.clone()
        records = self._todo_records
        para.with_code(
            code=records[
                (records[FREQ] == para.comb.freq)
                & (records[SOURCE] == para.comb.source)
                & (records[FUQUAN] == para.comb.fuquan)
            ].iloc[0, :][CODE]
        )
        table_name = TableNameTool.get_by_code(para=para)
        meta = self._operator.get_meta(name=table_name)
        meta = pd.concat([meta, ADD_META])
        return meta

    def _reform_n_save_data(self, df: DataFrame) -> None:
        """
        分组进行reform操作

        :param df:                  某一组待下载记录
        :return:
        """
        row = df.iloc[0, :]
        ReformObject.initialize(
            operator=self._operator,
            comb=CombType(source=row[SOURCE], freq=row[FREQ], fuquan=row[FUQUAN]),
        )
        for row in df.itertuples(index=False):
            obj = ReformObject(row=row)
            obj.log_start()
            obj.select_data()
            self._push_cache(obj)
        self._pop_caches()

    def _push_cache(self, obj: ReformObject):
        """
        把obj压入cache

        :param obj:
        :return:
        """
        self._data_caches.append(obj)
        self._data_cache_num += obj.data_num
        if len(self._data_caches) > OBJ_THLD and self._data_cache_num > DATA_THLD:
            self._pop_caches()

    def _pop_caches(self):
        """
        弹出所有cache

        :return:
        """
        if list_not_valid(self._data_caches):
            return
        comb = self._data_caches[0].para.comb
        data = pd.concat_safe([x.data for x in self._data_caches])
        if dataframe_is_valid(data):
            key = DATE if DATE in data.columns else DATETIME
            data[MONTH_START] = data[key].apply(lambda x: DateTime(x.year, x.month, 1))
            data.groupby(by=[MONTH_START]).apply(
                lambda subgroup: self._save_to_database(df=subgroup, comb=comb)
            )
        self._save_reform_records()
        self._data_caches = []

    def _save_to_database(self, df: DataFrame, comb: CombType) -> None:
        """
        把数据存储到数据库

        :param df:                  需要存储的数据
        :param comb:                source, freq, fuquan
        :return:
        """
        para = Para().with_comb(comb).with_start(df.iloc[0][MONTH_START])
        table_name_by_date = TableNameTool.get_by_date(para=para)
        del df[MONTH_START]
        try:
            self._operator.insert_data(name=table_name_by_date, df=df)
        except IntegrityError as e:
            if e.args[0] == 1062:
                logging.warning(e)
                meta = self._get_meta_cache(para=para)
                self._operator.try_insert_data(
                    name=table_name_by_date, df=df, meta=meta
                )
            else:
                raise e

    def _save_reform_records(self):
        """
        保存转换记录

        :return:
        """
        ls = []
        comb = self._data_caches[0].para.comb
        source, freq, fuquan = comb.source, comb.freq, comb.fuquan
        for data in self._data_caches:
            code = data.para.target.code
            total_span = data.total_span
            start, end = total_span.start, total_span.end
            ls.append([code, source, freq, fuquan, start, end])
        df = DataFrame(
            data=ls, columns=[CODE, SOURCE, FREQ, FUQUAN, START_DATE, END_DATE]
        )
        self._rf_recorder.save_to_database(df=df)
        for data in self._data_caches:
            data.log_end()

    # endregion


class ReformObject:
    _operator = None
    _comb = None

    @classmethod
    def initialize(cls, operator: Operator, comb: CombType):
        cls._operator = operator
        cls._comb = comb

    def __init__(self, row: tuple):
        self._para = (
            Para()
            .with_comb(self._comb)
            .with_code(getattr(row, CODE))
            .with_start_n_end(
                start=getattr(row, START_DATE), end=getattr(row, END_DATE)
            )
        )
        self._done_span = (
            None
            if pd.isna(getattr(row, DORCD_START))
            else DateSpan(getattr(row, DORCD_START), getattr(row, DORCD_END))
        )
        self._data = None
        self._data_num = 0

    @property
    def data_num(self):
        return self._data_num

    @property
    def data(self) -> Optional[DataFrame]:
        return self._data

    @property
    def para(self):
        return self._para

    @property
    def total_span(self):
        return self._para.span.add(self._done_span)

    def select_data(self) -> None:
        """
        按照指定的范围，读取数据

        :return:            None
        """

        table_name_by_code = TableNameTool.get_by_code(para=self._para)
        todo_ls = self._para.span.subtract(self._done_span)
        data = pd.concat_safe(
            [
                self._operator.select_data(name=table_name_by_code, span=span)
                for span in todo_ls
            ]
        )
        if dataframe_is_valid(data):
            data[CODE] = self._para.target.code
            self._data = data
            self._data_num = len(data)

    @property
    def _format_para(self) -> str:
        para = self._para
        return "{0} {1}info {2} {3} {4}-{5}".format(
            para.comb.source,
            para.comb.freq,
            para.target.code,
            para.comb.fuquan,
            para.span.start,
            para.span.end,
        )

    def log_start(self):
        Logger.info(f"Start to convert {self._format_para}.")

    def log_end(self):
        Logger.info(f"Successfully convert {self._format_para}")
