from buffett.analysis import Para
from buffett.analysis.study import ConvertStockMinuteAnalyst
from buffett.analysis.types import AnalystType
from buffett.common.tools import dataframe_is_valid
from buffett.download.types import SourceType, FreqType, FuquanType


def test_conv_min(self):
    """
    测试ConvertStockMinuteAnalyst

    :param self:        _calendar_handler
                        _dc_daily_handler
                        _bs_daily_handler
                        _th_daily_handler
                        _bs_minute_handler
                        _index_handler
                        _concept_handler
                        _industry_handler
                        _fhpg_handler
                        _daily_mtain
    :return:
    """
    row = self._bs_minute_handler.select_data(para=self._long_para).shape[0]
    analyst = ConvertStockMinuteAnalyst(stk_op=self._stk_rop, ana_op=self._operator)
    analyst.calculate(span=self._long_para.span)
    select_para = (
        Para()
        .with_source(SourceType.ANA)
        .with_freq(FreqType.MIN5)
        .with_fuquan(FuquanType.HFQ)
        .with_code("000001")
        .with_analysis(AnalystType.CONV)
    )
    row2 = analyst.select_data(select_para).shape[0]
    assert row == row2
