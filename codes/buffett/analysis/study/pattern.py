from typing import Optional

from buffett.adapter.pandas import pd, DataFrame, Series
from buffett.adapter.talib import PatternRecognize
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import DATETIME
from buffett.common.constants.col.analysis import EVENT, VALUE
from buffett.common.constants.meta.analysis import ANA_EVENT_META
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator


class PatternAnalyst(Analyst):
    def __init__(self, ana_op: Operator, stk_op: Operator):
        super(PatternAnalyst, self).__init__(
            stk_rop=stk_op,
            ana_rop=ana_op,
            ana_wop=ana_op.copy(),
            analyst=AnalystType.PATTERN,
            meta=ANA_EVENT_META,
        )

    def _calculate(self, para: Para) -> Optional[DataFrame]:
        """
        执行计算逻辑

        :param para:
        :return:
        """
        start = self._calendarman.query(para.span.start, offset=-5)
        select_para = para.clone().with_start(start)
        data = self._dataman.select_data(para=select_para)
        if dataframe_not_valid(data):
            self._logger.warning_calculate_end(para=para)
            return
        pattern = PatternRecognize.all(inputs=data)
        pattern = self._convert_pattern(pattern)
        return pattern

    @staticmethod
    def _convert_pattern(pattern: DataFrame) -> DataFrame:
        """
        将模式识别结果转换为事件表达结构

        :param pattern:
        :return:
        """

        def _filter(col: Series) -> DataFrame:
            col = col[col != 0]
            data = DataFrame({DATETIME: col.index, EVENT: col.name, VALUE: col.values})
            return data

        pattern = pd.concat([_filter(pattern[x]) for x in pattern.columns])
        return pattern
