from abc import abstractmethod
from typing import Optional

from buffett.adapter.numpy import NAN
from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.wellknown import tqdm
from buffett.common.constants.col import (
    DATE,
    SOURCE,
    FUQUAN,
    FREQ,
    START_DATE,
    END_DATE,
)
from buffett.common.constants.col.my import DORCD_START, DORCD_END
from buffett.common.constants.col.target import CODE
from buffett.common.pendulum import DateSpan, Date
from buffett.common.tools import dataframe_is_valid
from buffett.download import Para
from buffett.download.handler import Handler
from buffett.download.handler.tools import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FuquanType, FreqType


class MediumHandler(Handler):
    def __init__(
        self,
        operator: Operator,
        target_list_handler: Handler,
        recorder: DownloadRecorder,
        source: SourceType,
        fuquan: FuquanType,
        freq: FreqType,
        field_code: str,
        field_name: str,
    ):
        super(MediumHandler, self).__init__(operator)
        self._target_list_handler = target_list_handler
        self._recorder = recorder
        self._source = source
        self._fuquan = fuquan
        self._freq = freq
        self._CODE = field_code
        self._NAME = field_name

    def obtain_data(self, span: DateSpan) -> None:
        todo_records = self._get_todo_records(span=span)
        with tqdm(total=len(todo_records)) as pbar:
            for row in todo_records.itertuples(index=False):
                para = Para.from_tuple(row)
                data = self._download(row=row)
                data = self._filter(row=row, df=data)
                self._save_to_database(df=data, para=para)
                self._recorder.save(para=para)
                pbar.update(1)

    def _get_todo_records(self, span: DateSpan):
        """
        获取待下载记录

        :param: span
        :return:
        """
        item_list = self._target_list_handler.select_data()
        todo_records = DataFrame(
            {
                self._CODE: item_list[self._CODE],
                self._NAME: item_list[self._NAME],
                FREQ: self._freq,
                FUQUAN: self._fuquan,
                SOURCE: self._source,
                START_DATE: span.start,
                END_DATE: Date.today() if span.end > Date.today() else span.end,
            }
        )

        done_records = self._recorder.select_data()
        if dataframe_is_valid(done_records):
            # recorder存放时没有区分，都叫做CODE，因此需要进行重命名
            done_records = done_records.rename(columns={CODE: self._CODE})
            todo_records = pd.subtract(todo_records, done_records)
            done_records = done_records.rename(
                columns={START_DATE: DORCD_START, END_DATE: DORCD_END}
            )
            todo_records = pd.merge(
                todo_records,
                done_records,
                on=[self._CODE, SOURCE, FUQUAN, FREQ],
                how="left",
            )
        else:
            todo_records[DORCD_START] = NAN
            todo_records[DORCD_END] = NAN
        return todo_records

    @staticmethod
    def _filter(row: tuple, df: DataFrame) -> DataFrame:
        """
        按指定范围过滤数据

        :param row:
        :param df:
        :return:
        """
        todo_span = DateSpan(start=getattr(row, START_DATE), end=getattr(row, END_DATE))
        todo_ls = [todo_span]
        if not pd.isna(getattr(row, DORCD_END)):
            done_span = DateSpan(
                start=getattr(row, DORCD_START), end=getattr(row, DORCD_END)
            )
            todo_ls = todo_span.subtract(done_span)
        df = df[df[DATE].apply(lambda x: any(y.is_inside(x) for y in todo_ls))]
        return df

    @abstractmethod
    def _download(self, row: tuple) -> DataFrame:
        pass

    @abstractmethod
    def _save_to_database(self, df: DataFrame, para: Para) -> None:
        pass

    @abstractmethod
    def select_data(self, para: Para) -> Optional[DataFrame]:
        para = (
            Para()
            .with_code(para.target.code)
            .with_source(self._source)
            .with_freq(self._freq)
            .with_fuquan(self._fuquan)
        )

        table_name = TableNameTool.get_by_code(para=para)
        return self._operator.select_data(name=table_name)
