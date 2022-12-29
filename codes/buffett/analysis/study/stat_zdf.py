from typing import Optional

from zdf_stat import stat_past_with_period  # ignore this pycharm error

from buffett.adapter.numpy import np
from buffett.adapter.pandas import DataFrame
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst
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
from buffett.common.constants.meta.analysis import ANALY_ZDF_META
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
    def __init__(self, ana_op: Operator, stk_op: Operator):
        super(StatZdfAnalyst, self).__init__(
            stk_rop=stk_op,
            ana_rop=ana_op,
            ana_wop=ana_op.copy(),
            analyst=AnalystType.STAT_ZDF,
            meta=ANALY_ZDF_META,
            use_stock_minute=True,
        )

    def _calculate(self, para: Para) -> Optional[DataFrame]:
        """
        计算统计量

        :param para:
        :return:
        """
        mul = 1 if para.comb.freq == FreqType.DAY else 48
        start = self._calendarman.query(para.span.start, offset=-20)
        end = self._calendarman.query(para.span.end, offset=20)
        select_para = para.clone().with_start_n_end(start, end)
        data = self._dataman.select_data(para=select_para)
        if dataframe_not_valid(data) or len(data) < 40 * mul:
            self._logger.warning_calculate_end(para=para)
            return
        stat = self._calculate_zdf(data, mul)
        return stat

    @classmethod
    def _calculate_zdf(cls, df: DataFrame, mul: int) -> DataFrame:
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
