from buffett.analysis.study import FuquanAnalystV2
from buffett.common.constants.meta.analysis import FQ_FAC_META
from buffett.common.constants.table import FQ_FAC_V2
from buffett.common.tools import dataframe_is_valid


def test_fuquan_factor(self):
    """
    测试FuquanAnalyst

    :return:
    """
    handler = FuquanAnalystV2(
        stk_rop=self._stk_rop, ana_rop=self._ana_rop, ana_wop=self._ana_wop
    )
    handler.calculate(span=self._long_para.span)
    data = self._ana_rop.select_data(name=FQ_FAC_V2, meta=FQ_FAC_META)
    assert dataframe_is_valid(data)
