from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst, AnalystWorker
from buffett.analysis.study.fuquan import FuquanAnalystV2
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import CJL, CJE
from buffett.common.constants.meta.handler import BS_MINUTE_META
from buffett.common.tools import dataframe_not_valid
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.mysql import Operator
from buffett.download.types import FuquanType


class ConvertStockMinuteHfqAnalyst(Analyst):
    def __init__(self, ana_rop: Operator, ana_wop: Operator, stk_rop: Operator):
        super(ConvertStockMinuteHfqAnalyst, self).__init__(
            stk_rop=stk_rop,
            ana_rop=ana_rop,
            ana_wop=ana_wop,
            analyst=AnalystType.CONV,
            meta=BS_MINUTE_META,
            Worker=ConvertStockMinuteAnalystHfqWorker,
            use_stock=False,
            use_stock_minute=True,
            use_index=False,
            use_concept=False,
            use_industry=False,
        )


class ConvertStockMinuteAnalystHfqWorker(AnalystWorker):
    def __init__(
        self,
        stk_rop: Operator,
        ana_rop: Operator,
        ana_wop: Operator,
        analyst: AnalystType,
        meta: DataFrame,
    ):
        super(ConvertStockMinuteAnalystHfqWorker, self).__init__(
            stk_rop=stk_rop,
            ana_rop=ana_rop,
            ana_wop=ana_wop,
            analyst=analyst,
            meta=meta,
        )
        self._fuquan_analyst = FuquanAnalystV2(
            ana_rop=ana_rop, ana_wop=ana_wop, stk_rop=stk_rop
        )
        self._calendar = CalendarHandler(operator=stk_rop).select_data(
            index=False, to_datetimes=True
        )

    def _calculate(self, para: Para) -> Optional[DataFrame]:
        """
        执行计算逻辑

        :param para:
        :return:
        """
        # S0: 准备数据
        bfq_info = self._dataman.select_data(
            para=para.clone().with_fuquan(FuquanType.BFQ),
            index=True,
        )
        if dataframe_not_valid(bfq_info):
            self._logger.warning_end(para=para)
            return
        # S1: 计算
        conv_info = self._fuquan_analyst.reform_to_hfq(
            code=para.target.code, df=bfq_info
        )
        conv_info[[CJL, CJE]] = bfq_info[[CJL, CJE]]
        return conv_info.reset_index()
