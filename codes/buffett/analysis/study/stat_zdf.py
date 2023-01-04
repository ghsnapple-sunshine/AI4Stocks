from typing import Optional

from zdf_stat import stat_past_with_period  # ignore this pycharm error

from buffett.adapter.numpy import np
from buffett.adapter.pandas import DataFrame
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst, AnalystWorker
from buffett.analysis.study.supporter import CalendarManager
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import CLOSE, HIGH, LOW, DATETIME
from buffett.common.constants.col.analysis import (
    DF5_PCT99,
    DF5_PCT95,
    DF5_PCT90,
    ZF5_PCT90,
    ZF5_PCT95,
    ZF5_PCT99,
    DF5_MAX,
    ZF5_MAX,
    DF20_PCT99,
    DF20_PCT95,
    DF20_PCT90,
    ZF20_PCT90,
    ZF20_PCT95,
    ZF20_PCT99,
    DF20_MAX,
    ZF20_MAX,
)
from buffett.common.constants.meta.analysis import ANA_ZDF_META
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator
from buffett.download.types import FreqType

CLOSEd, HIGHd, LOWd = 0, 1, 2
input_columns = [CLOSE, HIGH, LOW]
output_columns = [
    DF5_PCT99,
    DF5_PCT95,
    DF5_PCT90,
    ZF5_PCT90,
    ZF5_PCT95,
    ZF5_PCT99,
    DF5_MAX,
    ZF5_MAX,
    DF20_PCT99,
    DF20_PCT95,
    DF20_PCT90,
    ZF20_PCT90,
    ZF20_PCT95,
    ZF20_PCT99,
    DF20_MAX,
    ZF20_MAX,
]


class StatZdfAnalyst(Analyst):
    def __init__(self, ana_rop: Operator, ana_wop: Operator, stk_rop: Operator):
        super(StatZdfAnalyst, self).__init__(
            stk_rop=stk_rop,
            ana_rop=ana_rop,
            ana_wop=ana_wop,
            analyst=AnalystType.STAT_ZDF,
            meta=ANA_ZDF_META,
            Worker=StatZdfAnalystWorker,
            use_stock=True,
            use_stock_minute=True,
            use_index=True,
            use_concept=True,
            use_industry=True,
        )


class StatZdfAnalystWorker(AnalystWorker):
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
        super(StatZdfAnalystWorker, self).__init__(
            pid=pid,
            stk_rop=stk_rop,
            ana_rop=ana_rop,
            ana_wop=ana_wop,
            analyst=analyst,
            meta=meta,
            kwd=kwd,
        )
        self._calendarman = CalendarManager(datasource_op=stk_rop)

    def _calculate(self, para: Para) -> Optional[DataFrame]:
        """
        计算统计量

        :param para:
        :return:
        """
        mul = 1 if para.comb.freq == FreqType.DAY else 48
        start = para.span.start
        end = self._calendarman.query(para.span.end, offset=20)
        select_para = para.clone().with_start_n_end(start, end)
        data = self._dataman.select_data(para=select_para)
        if dataframe_not_valid(data) or len(data) < 40 * mul:
            self._logger.warning_calculate_end(para=para)
            return
        stat = self._calculate_zdf(data, mul)
        return stat

    @staticmethod
    def _calculate_zdf(df: DataFrame, mul: int) -> DataFrame:
        """
        分别计算5日和20日的统计量

        :param df:
        :param mul:
        :return:
        """
        arr = df[input_columns].values
        stat5 = stat_past_with_period(arr, period=5 * mul)
        stat20 = stat_past_with_period(arr, period=20 * mul)
        stat = np.concatenate([stat5, stat20], axis=1)
        data = DataFrame(data=stat, columns=output_columns)
        data[DATETIME] = df.index
        return data
