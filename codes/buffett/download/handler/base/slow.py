from abc import abstractmethod
from typing import Optional

from buffett.adapter import logging
from buffett.adapter.numpy import NAN
from buffett.adapter.pandas import DataFrame, pd
from buffett.common.constants.col import (
    FREQ,
    FUQUAN,
    SOURCE,
    START_DATE,
    END_DATE,
    DATE,
)
from buffett.common.constants.col.my import DORCD_START, DORCD_END
from buffett.common.constants.col.target import CODE
from buffett.common.error import ParamTypeError
from buffett.common.error.pre_step import PreStepError
from buffett.common.magic import get_class
from buffett.common.pendulum import DateSpan, Date, convert_datetime, Duration
from buffett.common.tools import dataframe_not_valid, list_not_valid, dataframe_is_valid
from buffett.download.handler import Handler
from buffett.download.handler.tools import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.para import Para
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import FreqType, SourceType, FuquanType


class SlowHandler(Handler):
    """
    实现多张表的下载，存储
    """

    def __init__(
        self,
        operator: Operator,
        target_list_handler: Handler,
        calendar_handler: Handler,
        recorder: DownloadRecorder,
        source: SourceType,
        fuquans: list[FuquanType],
        freq: FreqType,
        field_code: str,
        field_name: str,
    ):
        super(SlowHandler, self).__init__(operator)
        self._target_list_handler = target_list_handler
        self._calendar_handler = calendar_handler
        self._recorder = recorder
        self._source = source
        self._fuquans = fuquans
        self._freq = freq
        self._CODE = field_code
        self._NAME = field_name

    # region 公共方法
    def obtain_data(self, para: Para):
        if not isinstance(para.span, DateSpan):
            raise ParamTypeError("para.span", DateSpan)

        para = self._fix_para(para)

        target_list = self._target_list_handler.select_data()
        if dataframe_not_valid(target_list):
            raise PreStepError(get_class(self), get_class(self._target_list_handler))
        done_records = self._recorder.select_data()

        todo_records = self._get_todo_records(
            target_list=target_list, done_records=done_records, para=para
        )
        comb_records = self._get_comb_records(
            todo_records=todo_records, done_records=done_records
        )
        tbs = [
            self._download_n_save_1stock(row)
            for row in comb_records.itertuples(index=False)
        ]
        return tbs

    @abstractmethod
    def select_data(self, para: Para) -> DataFrame:
        pass

    # endregion

    # region 私有方法
    def _fix_para(self, para: Para) -> Optional[Para]:
        """
        修正para中的start和end

        :param para:
        :return:
        """
        start, end = para.span.start, para.span.end
        end = end if end > Date.today() else Date.today()

        calendar = self._calendar_handler.select_data()
        if dataframe_not_valid(calendar) or end - start > Duration(
            days=7
        ):  # 未获取到calendar则不做额外处理
            return para

        start_date = Date(start.year, start.month, start.day)
        dates = []
        while start_date < end:
            dates.append(start_date)
            start_date.add(days=1)
        dates = DataFrame({DATE: dates})

        dates = dates.join(calendar, how="inner", on=[DATE], rsuffix="_r")
        if dataframe_not_valid(dates):
            return

        start = convert_datetime(dates[DATE].min())
        end = convert_datetime(dates[DATE].max().add(days=1)).add(days=1)
        return para.clone().with_start_n_end(start, end)

    def _get_todo_records(
        self, target_list: DataFrame, done_records: DataFrame, para: Para
    ) -> DataFrame:
        """
        计算待下载的记录

        :param target_list:         标的清单
        :param done_records:        下载记录
        :param para:                span
        :return:                    待下载记录
        """
        fuquan_df = DataFrame(self._fuquans, columns=[FUQUAN])
        todo_records = pd.merge(target_list, fuquan_df, how="cross")
        todo_records[FREQ] = self._freq
        todo_records[SOURCE] = self._source
        todo_records[START_DATE] = para.span.start
        todo_records[END_DATE] = para.span.end
        if dataframe_is_valid(done_records):
            done_records = done_records.rename(columns={CODE: self._CODE})
            todo_records = pd.subtract(todo_records, done_records)
        return todo_records

    def _get_comb_records(
        self, todo_records: DataFrame, done_records: DataFrame
    ) -> DataFrame:
        """
        将待下载记录和已下载记录进行拼接

        :param todo_records:
        :param done_records:
        :return:
        """
        if dataframe_not_valid(done_records):
            todo_records[DORCD_START] = NAN
            todo_records[DORCD_END] = NAN
            return todo_records

        done_records = done_records.rename(
            columns={CODE: self._CODE, START_DATE: DORCD_START, END_DATE: DORCD_END}
        )
        todo_records = pd.merge(
            todo_records,
            done_records,
            how="left",
            on=[self._CODE, FREQ, SOURCE, FUQUAN],
        )
        return todo_records

    def _download_n_save_1stock(self, row: tuple) -> None:
        """
        下载某一只股票

        :param row:
        :return:
        """
        para = Para.from_tuple(tup=row)

        todo_span = DateSpan(getattr(row, START_DATE), getattr(row, END_DATE))
        if pd.isna(getattr(row, DORCD_START)):
            done_span = None
            data = self._download(para=para.with_span(todo_span))
        else:
            done_span = DateSpan(getattr(row, DORCD_START), getattr(row, DORCD_END))
            todo_ls = todo_span.subtract(done_span)
            if list_not_valid(todo_ls):
                self._log_already_downloaded(para=para)
                return
            data = pd.concat_safe(
                [self._download(para=para.with_span(span)) for span in todo_ls]
            )

        table_name = TableNameTool.get_by_code(para=para)
        if dataframe_is_valid(data):
            self._save_to_database(table_name=table_name, df=data)
        total_span = todo_span if done_span is None else todo_span.add(done_span)
        self._recorder.save(para=para.with_span(total_span))
        self._log_success_download(para=para)

    @abstractmethod
    def _download(self, para: Para) -> DataFrame:
        """
        根据para中指定的条件下载数据

        :param para:
        :return:
        """
        pass

    @abstractmethod
    def _save_to_database(self, table_name: str, df: DataFrame) -> None:
        """
        将下载的数据存放到数据库

        :param table_name:
        :param df:
        :return:
        """
        pass

    @classmethod
    def _log_success_download(cls, para: Para):
        logging.info(
            "Successfully Download Stock {0} {1} {2} {3}".format(
                para.comb.freq, para.target.code, para.target.name, para.comb.fuquan
            )
        )

    @classmethod
    def _log_already_downloaded(cls, para: Para):
        logging.info(
            "Have already Downloaded Stock {0} {1} {2} {3}".format(
                para.comb.freq, para.target.code, para.target.name, para.comb.fuquan
            )
        )


# endregion
