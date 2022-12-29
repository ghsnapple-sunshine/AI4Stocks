from buffett.analysis.study import FuquanAnalyst
from buffett.common.constants.meta.handler.definition import FQ_FAC_META
from buffett.common.constants.table import FQ_FAC
from buffett.common.tools import dataframe_is_valid


def test_fuquan_factor(self):
    """
    测试FuquanAnalyst

    :return:
    """
    handler = FuquanAnalyst(stk_op=self._datasource_op, ana_op=self._operator)
    handler.calculate(span=self._long_para.span)
    data = self._datasource_op.select_data(name=FQ_FAC, meta=FQ_FAC_META)
    assert dataframe_is_valid(data)
