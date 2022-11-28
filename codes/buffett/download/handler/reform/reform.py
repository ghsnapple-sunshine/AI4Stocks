from typing import Optional

from pymysql import IntegrityError

from buffett.adapter import logging
from buffett.adapter.numpy import NAN, np
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
from buffett.common.pendulum import DateSpan, DateTime
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.download.handler.tools.table_name import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType
from buffett.download.para import Para
from buffett.download.recorder.dl_recorder import DownloadRecorder as DRecorder
from buffett.download.recorder.rf_recorder import ReformRecorder as RRecorder
from buffett.download.types import CombType

_ADD_META = create_meta(meta_list=[[CODE, ColType.CODE, AddReqType.KEY]])

GROUP = "group_id"
GROUP_SIZE = 30


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
        self._stock_group = DataFrame()

    # region 公共方法
    def reform_data(self) -> None:
        """
        把按照Code组织的股票数据转换成按照Date组织

        :return:
        """
        self._get_todo_records()
        if dataframe_not_valid(self._todo_records):
            return

        self._get_group()
        self._create_tables()
        comb_records = self._get_comb_records()

        comb_records.groupby(
            by=[GROUP, FREQ, SOURCE, FUQUAN, START_DATE, END_DATE]
        ).apply(lambda group: self._reform_n_save_data(group))

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

    def _get_group(self) -> None:
        """
        将股票代码分组

        :return:
        """
        stock_list = self._todo_records[[CODE]].drop_duplicates()
        row = stock_list.shape[0]
        group = np.linspace(0, row - 1, row, dtype=int)
        group = [x // GROUP_SIZE for x in group]
        stock_list[GROUP] = group
        self._stock_group = stock_list

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

        todo_records = pd.merge(todo_records, self._stock_group, how="left", on=[CODE])
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

        for row in todo_tables.itertuples(index=False):
            para = Para.from_tuple(row).with_start(getattr(row, MONTH_START))
            table_name = TableNameTool.get_by_date(para=para)
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
        record = records[
            (records[FREQ] == para.comb.freq)
            & (records[SOURCE] == para.comb.source)
            & (records[FUQUAN] == para.comb.fuquan)
        ].iloc[0, :]
        para.with_code(code=record[CODE])
        table_name = TableNameTool.get_by_code(para=para)
        meta = self._operator.get_meta(name=table_name)
        meta = pd.concat([meta, _ADD_META])
        return meta

    def _reform_n_save_data(self, df: DataFrame) -> None:
        """
        分组进行reform操作

        :param df:                  某一组待下载记录
        :return:
        """
        row = df.iloc[0]
        para = Para.from_series(row)
        span = DateSpan(row[START_DATE], row[END_DATE])
        group_desc = "group {0}info {1}({2}-{3}) {4} {5}".format(
            para.comb.freq,
            row[GROUP],
            df[CODE].min(),
            df[CODE].max(),
            para.comb.fuquan,
            span,
        )
        logging.info(f"Start to convert {group_desc}")

        data = pd.concat_safe(
            [
                self._get_required_data(row=row, para=para)
                for row in df.itertuples(index=False)
            ]
        )
        if dataframe_is_valid(data):
            key = DATE if DATE in data.columns else DATETIME
            data[MONTH_START] = data[key].apply(lambda x: DateTime(x.year, x.month, 1))
            data.groupby(by=[MONTH_START]).apply(
                lambda subgroup: self._save_2_database(df=subgroup, para=para)
            )

        self._save_reform_records(df=df)

        logging.info(f"Successfully convert {group_desc}")

    def _get_required_data(self, row: tuple, para: Para) -> Optional[DataFrame]:
        """
        按照指定的范围，读取数据

        :param row:                 读取数据的范围
        :param para                 freq, source, fuquan
        :return:
        """
        para = para.with_code(getattr(row, CODE))
        table_name_by_code = TableNameTool.get_by_code(para=para)
        todo_span = DateSpan(start=getattr(row, START_DATE), end=getattr(row, END_DATE))
        if pd.isna(getattr(row, DORCD_START)):
            todo_ls = [todo_span]
        else:
            done_span = DateSpan(getattr(row, DORCD_START), getattr(row, DORCD_END))
            todo_ls = todo_span.subtract(done_span)
        data = pd.concat_safe(
            [
                self._operator.select_data(name=table_name_by_code, span=span)
                for span in todo_ls
            ]
        )
        if dataframe_is_valid(data):
            data[CODE] = para.target.code
        return data

    def _save_2_database(self, df: DataFrame, para: Para) -> None:
        """
        把数据存储到数据库

        :param df:                  需要存储的数据
        :param para:                freq, source, fuquan
        :return:
        """
        para = para.with_start(df.iloc[0][MONTH_START])
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

    def _save_reform_records(self, df: DataFrame):
        """
        保存转换记录

        :param df:
        :return:
        """
        df = pd.concat(  # Assure safe
            [
                ReformHandler._get_reform_record(row)
                for row in df.itertuples(index=False)
            ]
        )
        self._rf_recorder.save_to_database(df=df)

    @classmethod
    def _get_reform_record(cls, row: tuple) -> DataFrame:
        """
        获取转换记录（待转换+已转换）

        :param row:
        :return:
        """
        todo_span = DateSpan(getattr(row, START_DATE), getattr(row, END_DATE))
        if not pd.isna(getattr(row, DORCD_START)):
            done_span = DateSpan(getattr(row, DORCD_START), getattr(row, DORCD_END))
            todo_span = todo_span.add(done_span)
        return DataFrame(
            [
                {
                    CODE: getattr(row, CODE),
                    FREQ: getattr(row, FREQ),
                    SOURCE: getattr(row, SOURCE),
                    FUQUAN: getattr(row, FUQUAN),
                    START_DATE: todo_span.start,
                    END_DATE: todo_span.end,
                }
            ]
        )

    # endregion


"""
class ReformHandlerLogger:
    def __init__(self):

    def handle_group_start(self, group: DataFrame):
        group = group[_GROUP].iloc[0]
        stock_list =
        logging.info('start to handle group {0}')
"""
