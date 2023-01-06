from typing import Optional

from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst, AnalystWorker
from buffett.analysis.study.fuquan_v2 import FuquanAnalystV2
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import CJL, CJE, DATETIME, CLOSE, OPEN, HIGH, LOW
from buffett.common.constants.meta.handler import BS_MINUTE_META
from buffett.common.tools import dataframe_not_valid
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.mysql import Operator
from buffett.download.types import FuquanType, SourceType


class ConvertStockMinuteAnalyst(Analyst):
    def __init__(self, ana_rop: Operator, ana_wop: Operator, stk_rop: Operator):
        super(ConvertStockMinuteAnalyst, self).__init__(
            stk_rop=stk_rop,
            ana_rop=ana_rop,
            ana_wop=ana_wop,
            analyst=AnalystType.CONV,
            meta=BS_MINUTE_META,
            Worker=ConvertStockMinuteAnalystWorker,
            use_stock=False,
            use_stock_minute=True,
            use_index=False,
            use_concept=False,
            use_industry=False,
        )


class ConvertStockMinuteAnalystWorker(AnalystWorker):
    def __init__(
        self,
        stk_rop: Operator,
        ana_rop: Operator,
        ana_wop: Operator,
        analyst: AnalystType,
        meta: DataFrame,
        kwd: str,
        pid: int,
    ):
        super(ConvertStockMinuteAnalystWorker, self).__init__(
            pid=pid,
            stk_rop=stk_rop,
            ana_rop=ana_rop,
            ana_wop=ana_wop,
            analyst=analyst,
            meta=meta,
            kwd=kwd,
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
            para=para.clone().with_source(SourceType.BS).with_fuquan(FuquanType.BFQ),
            index=False,
        )
        if dataframe_not_valid(bfq_info):
            self._logger.warning_calculate_end(para=para)
            return
        # S1: 计算
        _st, _ed = bfq_info.loc[0, DATETIME], bfq_info.loc[len(bfq_info) - 1, DATETIME]
        _c1 = self._calendar[
            (self._calendar[DATETIME] >= _st) & (self._calendar[DATETIME] <= _ed)
        ]
        _c2 = pd.merge(_c1, bfq_info, how="left", on=[DATETIME])
        _c2.loc[_c2[CLOSE] == 0, [OPEN, CLOSE, HIGH, LOW, CJL, CJE]] = float("nan")
        _c2[CLOSE] = _c2[CLOSE].ffill()
        _f1 = pd.isna(_c2[OPEN])
        _c2.loc[_f1, [OPEN, HIGH, LOW]] = _c2.loc[_f1, CLOSE]
        _c2.loc[_f1, [CJL, CJE]] = 0
        _c3 = _c2.set_index(DATETIME)
        _c4 = self._fuquan_analyst.reform_to_hfq(code=para.target.code, df=_c3)
        _c4[[CJL, CJE]] = _c3[[CJL, CJE]]
        conv_info = _c4[pd.notna(_c4[CLOSE])].reset_index()
        return conv_info
