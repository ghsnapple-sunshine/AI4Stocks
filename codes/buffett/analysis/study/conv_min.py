from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst
from buffett.analysis.study.fuquan_v2 import FuquanAnalystV2
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import CJL, CJE
from buffett.common.constants.meta.handler import BS_MINUTE_META
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator
from buffett.download.types import FuquanType, SourceType


class ConvertStockMinuteAnalyst(Analyst):
    def __init__(self, ana_op: Operator, stk_op: Operator):
        super(ConvertStockMinuteAnalyst, self).__init__(
            stk_rop=stk_op,
            ana_rop=ana_op,
            ana_wop=ana_op.copy(),
            analyst=AnalystType.CONV,
            meta=BS_MINUTE_META,
            use_stock=False,
            use_stock_minute=True,
            use_index=False,
            use_concept=False,
            use_industry=False,
        )
        self._fuquan_analyst = FuquanAnalystV2(ana_op=ana_op, stk_op=stk_op)

    def _calculate(self, para: Para) -> Optional[DataFrame]:
        """
        执行计算逻辑

        :param para:
        :return:
        """
        bfq_info = self._dataman.select_data(
            para=para.clone().with_source(SourceType.BS).with_fuquan(FuquanType.BFQ)
        )
        if dataframe_not_valid(bfq_info):
            self._logger.warning_calculate_end(para=para)
            return
        hfq_info = self._fuquan_analyst.reform_to_hfq(
            code=para.target.code, df=bfq_info
        )
        hfq_info[[CJL, CJE]] = bfq_info.loc[:, [CJL, CJE]]
        hfq_info = hfq_info.reset_index()
        return hfq_info
