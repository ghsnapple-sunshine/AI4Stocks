from buffett.adapter.pandas import pd, DataFrame, Series
from buffett.adapter.talib import PatternRecognize
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import (
    DATETIME,
)
from buffett.common.constants.col.analysis import EVENT, VALUE
from buffett.common.constants.meta.analysis import ANALY_EVENT_META
from buffett.download.mysql import Operator


class PatternAnalyst(Analyst):
    def __init__(self, select_op: Operator, insert_op: Operator):
        super(PatternAnalyst, self).__init__(
            select_op=select_op,
            insert_op=insert_op,
            analyst=AnalystType.PATTERN,
            use_economy=True,
            offset=5,
            meta=ANALY_EVENT_META,
        )

    def _calculate(self, para: Para):
        """
        执行计算逻辑

        :param para:
        :return:
        """

        start = self._calendarman.query(para.span.start, offset=self._offset)
        select_para = para.clone().with_start(start)
        data = self._dataman.select_data(para=select_para, use_economy=self._use_economy)

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
