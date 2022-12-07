from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.analysis.para import Para
from buffett.analysis.types import AnalysisType
from buffett.common.constants.col import FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from buffett.common.constants.col.analysis import ANALYSIS
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.analysis import ANALY_RCD_META
from buffett.common.constants.table import ANALY_RCD
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator
from buffett.download.recorder.simple_recorder import SimpleRecorder
from buffett.download.types import FreqType, FuquanType, SourceType


class AnalysisRecorder(SimpleRecorder):
    def __init__(self, operator: Operator):
        super(AnalysisRecorder, self).__init__(
            operator=operator, table_name=ANALY_RCD, meta=ANALY_RCD_META
        )

    def save(self, para: Para) -> None:
        """
        基于para保存

        :param para:
        :return:
        """
        ls = [
            [
                para.target.code,
                para.comb.freq,
                para.comb.fuquan,
                para.comb.source,
                para.analysis,
                para.span.start,
                para.span.end,
            ]
        ]
        cols = [CODE, FREQ, FUQUAN, SOURCE, ANALYSIS, START_DATE, END_DATE]
        data = DataFrame(data=ls, columns=cols)
        self.save_to_database(df=data)

    def select_data(self, *args, **kwargs) -> Optional[DataFrame]:
        """
        获取Recorder所有记录

        :return:
        """
        df = self._operator.select_data(self._TABLE_NAME)
        if dataframe_not_valid(df):
            return
        df[ANALYSIS] = df[ANALYSIS].apply(lambda x: AnalysisType(x))
        df[FREQ] = df[FREQ].apply(lambda x: FreqType(x))
        df[FUQUAN] = df[FUQUAN].apply(lambda x: FuquanType(x))
        df[SOURCE] = df[SOURCE].apply(lambda x: SourceType(x))
        return df
