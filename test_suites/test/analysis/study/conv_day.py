from buffett.analysis import Para
from buffett.analysis.study import ConvertStockDailyAnalyst
from buffett.analysis.types import AnalystType
from buffett.common.tools import dataframe_is_valid
from buffett.download.types import SourceType, FreqType, FuquanType


def test_conv_day(self):
    """
    测试ConvertStockDailyAnalyst

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
    handler = ConvertStockDailyAnalyst(
        stk_rop=self._stk_rop, ana_rop=self._ana_rop, ana_wop=self._ana_wop
    )
    handler.calculate(span=self._long_para.span)
    select_para = (
        Para()
        .with_source(SourceType.ANA)
        .with_freq(FreqType.DAY)
        .with_fuquan(FuquanType.HFQ)
        .with_code("000001")
        .with_analysis(AnalystType.CONV)
    )
    data = handler.select_data(select_para)
    assert dataframe_is_valid(data)
