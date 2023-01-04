from buffett.analysis import Para
from buffett.analysis.study import PatternAnalyst
from buffett.analysis.types import AnalystType
from buffett.common.tools import dataframe_is_valid
from buffett.download.types import SourceType, FreqType, FuquanType


def test_pattern(self):
    """
    测试PatternAnalyst

    :return:
    """
    handler = PatternAnalyst(stk_rop=self._stk_rop, ana_rop=self._operator)
    handler.calculate(span=self._long_para.span)
    select_para = (
        Para()
        .with_source(SourceType.ANA)
        .with_freq(FreqType.DAY)
        .with_fuquan(FuquanType.HFQ)
        .with_code("000001")
        .with_analysis(AnalystType.PATTERN)
    )
    data = handler.select_data(select_para)
    assert dataframe_is_valid(data)
