from buffett.analysis import Para
from buffett.analysis.study import StatZdfAnalyst
from buffett.analysis.types import AnalystType
from buffett.common.tools import dataframe_is_valid
from buffett.download.types import SourceType, FreqType, FuquanType


def test_stat(self):
    """
    测试StatAnalyst

    :return:
    """
    handler = StatZdfAnalyst(
        stk_rop=self._stk_rop, ana_rop=self._ana_rop, ana_wop=self._ana_wop
    )
    handler.calculate(span=self._long_para.span)
    select_para = (
        Para()
        .with_source(SourceType.ANA)
        .with_freq(FreqType.DAY)
        .with_fuquan(FuquanType.HFQ)
        .with_code("000001")
        .with_analysis(AnalystType.STAT_ZDF)
    )
    data = handler.select_data(select_para)
    assert dataframe_is_valid(data)
