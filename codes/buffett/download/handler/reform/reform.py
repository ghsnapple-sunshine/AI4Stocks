from pymysql import IntegrityError

from buffett.adapter import logging
from buffett.adapter.numpy import NAN, np
from buffett.adapter.pandas import DataFrame, pd, Series
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
from buffett.common.constants.col.stock import CODE
from buffett.common.pendelum import DateSpan, convert_datetime, DateTime
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.download.handler.tools.table_name import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType
from buffett.download.para import Para
from buffett.download.recorder.dl_recorder import DownloadRecorder as DRecorder
from buffett.download.recorder.rf_recorder import ReformRecorder as RRecorder
from buffett.download.types import CombType

_ADD_META = create_meta(meta_list=[[CODE, ColType.CODE, AddReqType.KEY]])

_GROUP = "group_id"
_GROUP_SIZE = 30


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
            by=[_GROUP, FREQ, SOURCE, FUQUAN, START_DATE, END_DATE]
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
        # todo_records = todo_records.drop_duplicates()
        done_records = self._rf_recorder.get_data()
        if dataframe_is_valid(done_records):
            # self._done_records = done_records.drop_duplicates()
            self._done_records = done_records
            todo_records = pd.concat(
                [todo_records, self._done_records, self._done_records]
            ).drop_duplicates(keep=False)

        self._todo_records = todo_records

    def _get_group(self) -> None:
        """
        将股票代码分组

        :return:
        """
        stock_list = self._todo_records[[CODE]].drop_duplicates()
        row = stock_list.shape[0]
        group = np.linspace(0, row - 1, row, dtype=int)
        group = [x // _GROUP_SIZE for x in group]
        stock_list[_GROUP] = group
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
        todo_tables = self.create_multi_series(self._todo_records)
        if dataframe_is_valid(self._done_records):
            done_tables = self.create_multi_series(self._done_records)
            todo_tables = pd.concat(
                [todo_tables, done_tables, done_tables]
            ).drop_duplicates(keep=False)

        for index, row in todo_tables.iterrows():
            para = (
                Para()
                .with_freq(row[FREQ])
                .with_source(row[SOURCE])
                .with_fuquan(row[FUQUAN])
                .with_start(row[MONTH_START])
            )
            table_name = TableNameTool.get_by_date(para=para)
            meta = self._get_meta_cache(para=para)
            self._operator.create_table(name=table_name, meta=meta)

    @classmethod
    def create_multi_series(cls, records: DataFrame) -> DataFrame:
        """
        生成已下载记录/待下载记录的Mysql表信息

        :param records:             已下载记录/待下载记录
        :return:
        """
        records = records[
            [FREQ, SOURCE, FUQUAN, START_DATE, END_DATE]
        ].drop_duplicates()
        spans = records[[START_DATE, END_DATE]].drop_duplicates()
        series = pd.concat(
            [
                ReformHandler._create_single_series(span)
                for index, span in spans.iterrows()
            ]
        )
        tables = pd.merge(records, series, how="left", on=[START_DATE, END_DATE])
        return tables

    @classmethod
    def _create_single_series(cls, spans: Series) -> DataFrame:
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
            dates.append([start, end, month_start])
            month_start = month_start.add(months=1)
        return DataFrame(dates, columns=[START_DATE, END_DATE, MONTH_START])

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
        para = (
            Para()
            .with_freq(row[FREQ])
            .with_source(row[SOURCE])
            .with_fuquan(row[FUQUAN])
        )
        span = DateSpan(row[START_DATE], row[END_DATE])
        group_desc = "group {0}info {1}({2}-{3}) {4} {5}".format(
            para.comb.freq,
            row[_GROUP],
            df[CODE].min(),
            df[CODE].max(),
            para.comb.fuquan,
            span,
        )
        logging.info(f"Start to convert {group_desc}")

        data = pd.concat(
            [
                self._get_required_data(row=row, para=para)
                for index, row in df.iterrows()
            ]
        )
        key = DATE if DATE in data.columns else DATETIME
        data[MONTH_START] = data[key].apply(lambda x: DateTime(x.year, x.month, 1))
        data.groupby(by=[MONTH_START]).apply(
            lambda subgroup: self._save_2_database(df=subgroup, para=para)
        )

        self._save_reform_records(df=df)

        logging.info(f"Successfully convert {group_desc}")

    def _get_required_data(self, row: Series, para: Para) -> DataFrame:
        """
        按照指定的范围，读取数据

        :param row:                 读取数据的范围
        :param para                 freq, source, fuquan
        :return:
        """
        para = para.with_code(row[CODE])
        table_name_by_code = TableNameTool.get_by_code(para=para)
        todo_span = DateSpan(start=row[START_DATE], end=row[END_DATE])
        if pd.isna(row[DORCD_START]):
            todo_ls = [todo_span]
        else:
            done_span = DateSpan(start=row[DORCD_START], end=row[DORCD_END])
            todo_ls = todo_span.subtract(done_span)
        data = pd.concat(
            [
                self._operator.select_data(name=table_name_by_code, span=span)
                for span in todo_ls
            ]
        )
        data[CODE] = para.stock.code
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
        df = pd.concat(
            [ReformHandler._get_reform_record(row) for index, row in df.iterrows()]
        )
        self._rf_recorder.save_to_database(df=df)

    @classmethod
    def _get_reform_record(cls, row: Series) -> DataFrame:
        """
        获取转换记录（待转换+已转换）

        :param row:
        :return:
        """
        todo_span = DateSpan(row[START_DATE], row[END_DATE])
        if not pd.isna(row[DORCD_START]):
            done_span = DateSpan(start=row[DORCD_START], end=row[DORCD_END])
            todo_span = todo_span.add(done_span)
        return DataFrame(
            [
                [
                    row[CODE],
                    row[FREQ],
                    row[SOURCE],
                    row[FUQUAN],
                    todo_span.start,
                    todo_span.end,
                ]
            ],
            columns=[CODE, FREQ, SOURCE, FUQUAN, START_DATE, END_DATE],
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
