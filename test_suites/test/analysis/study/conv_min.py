from buffett.analysis import Para
from buffett.analysis.study import (
    ConvertStockMinuteHfqAnalyst,
    ConvertStockMinuteBfqAnalyst,
)
from buffett.analysis.types import AnalystType
from buffett.common.tools import dataframe_is_valid
from buffett.download.types import SourceType, FreqType, FuquanType


def test_conv_min(self):
    """
    测试ConvertStockMinuteBfqAnalyst和ConvertStockMinuteHfqAnalyst

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
                        _data_manager
    :return:
    """
    #
    bfq_analyst = ConvertStockMinuteBfqAnalyst(
        stk_rop=self._stk_rop, ana_rop=self._ana_rop, ana_wop=self._ana_wop
    )
    bfq_analyst.calculate(span=self._long_para.span)
    bfq_para = (
        Para()
        .with_source(SourceType.ANA)
        .with_freq(FreqType.MIN5)
        .with_fuquan(FuquanType.BFQ)
        .with_code("000001")
        .with_analysis(AnalystType.CONV)
    )
    bfq_data = self._data_manager.select_data(bfq_para)
    assert dataframe_is_valid(bfq_data)
    #
    hfq_analyst = ConvertStockMinuteHfqAnalyst(
        stk_rop=self._stk_rop, ana_rop=self._ana_rop, ana_wop=self._ana_wop
    )
    hfq_analyst.calculate(span=self._long_para.span)
    hfq_para = bfq_para.clone().with_fuquan(FuquanType.HFQ)
    hfq_data = self._data_manager.select_data(hfq_para)
    assert dataframe_is_valid(hfq_data)
