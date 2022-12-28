from buffett.analysis import Para
from buffett.analysis.study import ConvertStockDailyAnalyst
from buffett.analysis.types import AnalystType
from buffett.common.tools import dataframe_is_valid
from buffett.download.types import SourceType, FreqType, FuquanType


def test_conv_day(self):
    """
    测试ConvertStockDailyAnalyst

    :return:
    """
    handler = ConvertStockDailyAnalyst(
        datasource_op=self._datasource_op, operator=self._operator
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
