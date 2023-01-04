from typing import Optional

import numpy as np
import pandas as pd

from buffett.adapter.pandas import DataFrame
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
            index=True, to_datetimes=True
        )

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
        _b0 = pd.merge(self._calendar, bfq_info, how="inner", on=[DATETIME])
        _b0[CLOSE] = _b0[CLOSE].ffill()
        _b1 = np.vectorize(pd.isna)(_b0[OPEN])
        _b0.loc[_b1, [OPEN, HIGH, LOW]] = _b0.loc[_b1, CLOSE]
        _b0.loc[_b1, [CJL, CJE]] = 0
        hfq_info = self._fuquan_analyst.reform_to_hfq(code=para.target.code, df=_b0)
        hfq_info[[CJL, CJE]] = _b0.loc[:, [CJL, CJE]]
        hfq_info = hfq_info.reset_index()
        return hfq_info
