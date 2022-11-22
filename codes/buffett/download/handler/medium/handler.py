from abc import abstractmethod
from typing import Optional

from buffett.adapter.numpy import NAN
from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.wellknown import tqdm
from buffett.common import create_meta
from buffett.common.constants.col import (
    DATE,
    SYL,
    DSYL,
    SJL,
    SXL,
    DSXL,
    GXL,
    ZSZ,
    DGXL,
    SOURCE,
    FUQUAN,
    FREQ,
    START_DATE,
    END_DATE,
)
from buffett.common.constants.col.my import DORCD_START, DORCD_END
from buffett.common.constants.col.stock import CODE
from buffett.common.pendulum import DateTime, DateSpan
from buffett.common.tools import dataframe_is_valid
from buffett.download import Para
from buffett.download.handler import Handler
from buffett.download.handler.list import StockListHandler
from buffett.download.handler.tools import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.mysql.types import AddReqType, ColType
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FuquanType, FreqType

_RENAME = {
    "trade_date": DATE,
    "pe": SYL,
    "pe_ttm": DSYL,
    "pb": SJL,
    "ps": SXL,
    "ps_ttm": DSXL,
    "dv_ratio": GXL,
    "dv_ttm": DGXL,
    "total_mv": ZSZ,
}

_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [CODE, ColType.CODE, AddReqType.KEY],
        [SYL, ColType.FLOAT, AddReqType.NONE],
        [DSYL, ColType.FLOAT, AddReqType.NONE],
        [SJL, ColType.FLOAT, AddReqType.NONE],
        [SXL, ColType.FLOAT, AddReqType.NONE],
        [DSXL, ColType.FLOAT, AddReqType.NONE],
        [GXL, ColType.FLOAT, AddReqType.NONE],
        [DGXL, ColType.FLOAT, AddReqType.NONE],
        [ZSZ, ColType.FLOAT, AddReqType.NONE],
    ]
)


class MediumHandler(Handler):
    def __init__(
        self,
        operator: Operator,
        list_handler: Handler,
        recorder: DownloadRecorder,
        source: SourceType,
        fuquan: FuquanType,
        freq: FreqType,
    ):
        super(MediumHandler, self).__init__(operator)
        self._list_handler = list_handler
        self._recorder = recorder
        self._source = source
        self._fuquan = fuquan
        self._freq = freq

    def obtain_data(self, para: Para = None):
        todo_records = self._get_todo_records()
        with tqdm(total=len(todo_records)) as pbar:
            for tup in todo_records.itertuples(index=False):
                para = Para.from_tuple(tup)
                data = self._download(tup=tup)
                data = self._filter(tup=tup, df=data)
                self._save_to_database(df=data, para=para)
                self._recorder.save(para=para)
                pbar.update(1)

    def _get_todo_records(self):
        """
        获取待下载记录

        :return:
        """
        item_list = self._list_handler.select_data()
        todo_records = DataFrame(
            {
                CODE: item_list[CODE],
                FREQ: self._freq,
                FUQUAN: self._fuquan,
                SOURCE: self._source,
                START_DATE: DateTime(2000, 1, 1),
                END_DATE: DateTime.today(),
            }
        )
        done_records = self._recorder.select_data()
        if dataframe_is_valid(done_records):
            todo_records = pd.subtract(todo_records, done_records)
            done_records = done_records.rename(
                columns={START_DATE: DORCD_START, END_DATE: DORCD_END}
            )
            todo_records = pd.merge(
                todo_records, done_records, on=[CODE, SOURCE, FUQUAN, FREQ], how="left"
            )
        else:
            todo_records[DORCD_START] = NAN
            todo_records[DORCD_END] = NAN
        return todo_records

    def _filter(self, tup: tuple, df: DataFrame) -> DataFrame:
        """
        按指定范围过滤数据

        :param tup:
        :param df:
        :return:
        """
        todo_span = DateSpan(start=getattr(tup, START_DATE), end=getattr(tup, END_DATE))
        if not pd.isna(getattr(tup, DORCD_END)):
            done_span = DateSpan(
                start=getattr(tup, DORCD_START), end=getattr(tup, DORCD_END)
            )
            todo_span = todo_span.subtract(done_span)[0]  # 因为其特殊性，不可能出现首尾都需要下载
        df = df[df[DATE].apply(lambda x: todo_span.is_inside(x))]
        return df

    @abstractmethod
    def _download(self, tup: tuple) -> DataFrame:
        pass

    @abstractmethod
    def _save_to_database(self, df: DataFrame, para: Para) -> None:
        pass

    @abstractmethod
    def select_data(self, para: Para) -> Optional[DataFrame]:
        super(MediumHandler, self).select_data(para=para)
        para = (
            para.clone()
            .with_source(self._source)
            .with_freq(self._freq)
            .with_fuquan(self._fuquan)
        )
        table_name = TableNameTool.get_by_code(para=para)
        return self._operator.select_data(name=table_name)
