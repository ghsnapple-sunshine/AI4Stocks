from abc import abstractmethod
from typing import Optional

from buffett.adapter.error.data_source import DataSourceError
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
from buffett.common.error import ParamTypeError, PreStepError
from buffett.common.interface import ProducerConsumer
from buffett.common.logger import Logger
from buffett.common.magic import get_class, empty_method
from buffett.common.pendulum import DateSpan, Date, convert_datetime, Duration
from buffett.common.tools import dataframe_not_valid, list_not_valid, dataframe_is_valid
from buffett.common.wrapper import Wrapper
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
        self._calendar = None
        self._fix_cache = {}

    def obtain_data(self, para: Para):
        if not isinstance(para.span, DateSpan):
            raise ParamTypeError("para.span", DateSpan)

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
        # 使用生产者/消费者模式，异步下载/保存数据
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._download_1target),
            consumer=Wrapper(self._save_1target),
            queue_size=30,
            args_map=comb_records.itertuples(index=False),
            task_num=len(comb_records),
        )
        prod_cons.run()
        return [None] * len(comb_records)  # 保持输出兼容性

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

    def _download_1target(self, row: tuple) -> Optional[tuple[Para, DataFrame]]:
        """
        下载某一个股票, [行业, 板块, 指数...]

        :param row:
        :return:
        """
        para = Para.from_tuple(tup=row)
        todo_span = para.span
        self._log_start_download(para=para)
        done_span = (
            None
            if pd.isna(getattr(row, DORCD_START))
            else DateSpan(getattr(row, DORCD_START), getattr(row, DORCD_END))
        )
        todo_ls = todo_span.subtract(done_span)
        if list_not_valid(todo_ls):
            self._log_already_downloaded(para=para)
            return
        try:
            data_ls = []
            for span in todo_ls:
                span = self._fix_span(span)  # Don't worry, it works.
                if span is not None:
                    data_ls.append(self._download(para=para.with_span(span)))
            data = pd.concat_safe(data_ls)
        except DataSourceError as e:  # 捕获到DataSourceError则继续下载（因为他总是会触发）。
            data = None
            Logger.error(e.msg)
        para = para.with_span(span=todo_span.add(done_span))
        return para, data

    def _fix_span(self, span: DateSpan) -> Optional[DateSpan]:
        """
        修正para中的start和end

        :param span:
        :return:
        """
        if span in self._fix_cache:
            return self._fix_cache[span]

        start, end = span.start, span.end
        end = end if end > Date.today() else Date.today()

        if end - start > Duration(days=7):
            self._fix_cache[span] = span
            return span
        self._load_calendar()
        if dataframe_not_valid(self._calendar):
            self._fix_cache[span] = span
            return span

        start_date = Date(start.year, start.month, start.day)
        dates = []
        while start_date < end:
            dates.append(start_date)
            start_date = start_date.add(days=1)
        dates = DataFrame({DATE: dates})

        dates = dates.join(self._calendar, how="inner", on=[DATE], rsuffix="_r")
        if dataframe_not_valid(dates):
            self._fix_cache[span] = None
            return
        fixed_span = DateSpan(
            start=convert_datetime(dates[DATE].min()),
            end=convert_datetime(dates[DATE].max().add(days=1)).add(days=1),
        )
        self._fix_cache[span] = fixed_span
        return fixed_span

    def _load_calendar(self):
        """
        加载calendar（只能使用一次）

        :return:
        """
        self._calendar = self._calendar_handler.select_data()
        self._load_calendar = empty_method

    @abstractmethod
    def _download(self, para: Para) -> Optional[DataFrame]:
        """
        根据para中指定的条件下载数据

        :param para:
        :return:
        """
        pass

    def _save_1target(self, obj: tuple[Para, DataFrame]):
        """
        保存某一只股票, [行业, 板块, 指数...]

        :param obj:
        :return:
        """
        if obj is None:
            return
        para, data = obj
        if dataframe_is_valid(data):
            table_name = TableNameTool.get_by_code(para=para)
            self._save_to_database(table_name=table_name, df=data)
        self._recorder.save(para=para)
        self._log_success_download(para=para)

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
    def _log_start_download(cls, para: Para):
        Logger.info(f"Start to Download {para}")

    @classmethod
    def _log_success_download(cls, para: Para):
        Logger.info(f"Successfully Download {para}")

    @classmethod
    def _log_already_downloaded(cls, para: Para):
        Logger.info(f"Have already Downloaded {para}")

    @abstractmethod
    def select_data(self, para: Para) -> Optional[DataFrame]:
        pass
