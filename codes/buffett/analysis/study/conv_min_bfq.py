from typing import Optional

from buffett.adapter.numpy import NAN, np
from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst, AnalystWorker
from buffett.analysis.study.tools import get_stock_list
from buffett.analysis.types import AnalystType
from buffett.analysis.types import CombExType
from buffett.common.constants.col import (
    DATE,
    DATETIME,
    OPEN,
    CLOSE,
    HIGH,
    LOW,
    SOURCE,
    FREQ,
    FUQUAN,
    CJL,
    CJE,
)
from buffett.common.constants.col.my import DAILY_SUFFIX, MIN5_SUFFIX
from buffett.common.constants.meta.handler import BS_MINUTE_META
from buffett.common.constants.trade import TRADE_TIMES
from buffett.common.pendulum import convert_datetime
from buffett.common.tools import dataframe_not_valid
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.mysql import Operator
from buffett.download.types import SourceType, FreqType, FuquanType

DAILY_COMB = CombExType(
    source=SourceType.ANA,
    freq=FreqType.DAY,
    fuquan=FuquanType.BFQ,
    analysis=AnalystType.CONV,
)
MIN5_COMB = CombExType(
    source=SourceType.BS,
    freq=FreqType.MIN5,
    fuquan=FuquanType.BFQ,
)
COLS = [OPEN, CLOSE, HIGH, LOW]
COLS_WITH_DATE = [DATE] + COLS
COLS_DAILY = [x + DAILY_SUFFIX for x in COLS]
COLS_MIN5 = [x + MIN5_SUFFIX for x in COLS]
ID = "id"
ADD = "add"
HIGH_DAILY, LOW_DAILY = HIGH + DAILY_SUFFIX, LOW + DAILY_SUFFIX
HIGH_MIN5, LOW_MIN5 = HIGH + MIN5_SUFFIX, LOW + MIN5_SUFFIX


class ConvertStockMinuteBfqAnalyst(Analyst):
    def __init__(self, ana_rop: Operator, ana_wop: Operator, stk_rop: Operator):
        super(ConvertStockMinuteBfqAnalyst, self).__init__(
            stk_rop=stk_rop,
            ana_rop=ana_rop,
            ana_wop=ana_wop,
            analyst=AnalystType.CONV,
            meta=BS_MINUTE_META,
            Worker=ConvertStockMinuteBfqAnalystWorkerV2,
            use_stock=False,
            use_stock_minute=True,
            use_index=False,
            use_concept=False,
            use_industry=False,
        )

    def _get_stock_list(self) -> Optional[DataFrame]:
        if not (self._use_stock or self._use_stock_minute):
            return
        stock_list = get_stock_list(operator=self._stk_rop)
        if dataframe_not_valid(stock_list):
            return
        stock_list[SOURCE] = SourceType.ANA
        stock_list[FREQ] = FreqType.MIN5
        stock_list[FUQUAN] = FuquanType.BFQ
        return stock_list


class ConvertStockMinuteBfqAnalystWorker(AnalystWorker):
    def __init__(
        self,
        stk_rop: Operator,
        ana_rop: Operator,
        ana_wop: Operator,
        analyst: AnalystType,
        meta: DataFrame,
    ):
        super(ConvertStockMinuteBfqAnalystWorker, self).__init__(
            stk_rop=stk_rop,
            ana_rop=ana_rop,
            ana_wop=ana_wop,
            analyst=analyst,
            meta=meta,
        )
        self._calendar = CalendarHandler(operator=self._stk_rop).select_data(
            index=False, to_datetimes=True
        )

    def _calculate(self, para: Para) -> Optional[DataFrame]:
        """
        执行计算逻辑

        :param para:
        :return:
        """
        # S0: 准备数据
        daily = self._dataman.select_data(
            para=para.clone().with_comb(DAILY_COMB), index=False
        )
        min5 = self._dataman.select_data(
            para=para.clone().with_comb(MIN5_COMB), index=False
        )
        if dataframe_not_valid(daily) or dataframe_not_valid(min5):
            self._logger.warning_end(para=para)
            return
        # S1: 刷新
        min5[DATE] = min5[DATETIME].dt.date
        daily_dict = dict(
            (k, v.reset_index(drop=True)) for k, v in daily.groupby(by=[DATE])
        )
        min5_dict = dict(
            (k, v.reset_index(drop=True)) for k, v in min5.groupby(by=[DATE])
        )
        min5_info_updated = pd.concat(
            [
                self._fix_min5_info(min5=min5_dict[k], daily=daily_dict[k])
                for k in min5_dict.keys()
            ]
        )
        return min5_info_updated[[DATETIME, OPEN, CLOSE, HIGH, LOW, CJL, CJE]]

    @staticmethod
    def _fix_min5_info(min5: DataFrame, daily: DataFrame) -> DataFrame:
        """
        根据日线的数据对分钟线进行修正

        :param min5:       某日的分钟线数据
        :param daily:      某日的日线数据
        :return:
        """
        #
        open_daily = daily.loc[0, OPEN]
        close_daily = daily.loc[0, CLOSE]
        high_daily = daily.loc[0, HIGH]
        low_daily = daily.loc[0, LOW]
        # fix datanum != 48
        if len(min5) > 48:
            date = convert_datetime(daily.loc[0, DATE])
            min5 = pd.merge(
                DataFrame({DATETIME: [x + date for x in TRADE_TIMES]}),
                min5,
                how="left",
                on=[DATETIME],
            )
        elif len(min5) < 48:
            date = convert_datetime(daily.loc[0, DATE])
            supp_info = pd.subtract(
                DataFrame({DATETIME: [x + date for x in TRADE_TIMES]}),
                min5[[DATETIME]],
            )
            supp_info[[CJL, CJE]] = 0
            min5 = (
                pd.concat([min5, supp_info])
                .sort_values(by=[DATETIME])
                .reset_index(drop=True)
            )
        # fix cjl = 0
        zero_cjl = min5[CJL] == 0
        if zero_cjl.any():
            min5.loc[zero_cjl, COLS] = NAN
            if pd.isna(min5.loc[0, CLOSE]):
                min5.loc[0, CLOSE] = open_daily
            min5[CLOSE] = min5[CLOSE].ffill()
            min5.loc[zero_cjl, [OPEN, HIGH, LOW]] = min5.loc[zero_cjl, CLOSE]
        # open
        open_min5 = min5.loc[0, OPEN]
        if open_daily != open_min5:
            min5.loc[0, OPEN] = open_daily
            min5.loc[0, HIGH] = max(min5.loc[0, HIGH], open_daily)
            min5.loc[0, LOW] = min(min5.loc[0, LOW], open_daily)
        # close
        close_min5 = min5.loc[47, CLOSE]
        if close_daily != close_min5:
            min5.loc[47, CLOSE] = close_daily
            min5.loc[47, HIGH] = max(min5.loc[47, HIGH], close_daily)
            min5.loc[47, LOW] = max(min5.loc[47, LOW], close_daily)
        # high
        high_min5_max = min5[HIGH].max()
        # 如果high_min5_max小于high_daily，修正一个；否则修正所有不合适的
        if high_min5_max < high_daily:
            fix_loc = min5[min5[HIGH] == high_min5_max].index[0]
            min5.loc[fix_loc, HIGH] = high_daily
        elif high_min5_max > high_daily:
            fix_loc = min5[HIGH] > high_daily
            min5.loc[fix_loc, HIGH] = high_daily
            min5.loc[fix_loc, [OPEN, CLOSE, LOW]] = min5.loc[
                fix_loc, [OPEN, CLOSE, LOW]
            ].applymap(lambda x: min(high_daily, x))
        # low
        low_min5_min = min5[LOW].min()
        # 如果low_min5_min大于low_daily，修正一个；否则修正所有不合适的
        if low_min5_min > low_daily:
            fix_loc = min5[min5[LOW] == low_min5_min].index[0]
            min5.loc[fix_loc, LOW] = low_daily
        elif low_min5_min < low_daily:
            fix_loc = min5[LOW] < low_daily
            min5.loc[fix_loc, LOW] = low_daily
            min5.loc[fix_loc, [OPEN, CLOSE, HIGH]] = min5.loc[
                fix_loc, [OPEN, CLOSE, HIGH]
            ].applymap(lambda x: max(low_daily, x))
        return min5


class ConvertStockMinuteBfqAnalystWorkerV2(ConvertStockMinuteBfqAnalystWorker):
    """
    向量法实现，运算更快
    """

    def _calculate(self, para: Para) -> Optional[DataFrame]:
        """
        执行计算逻辑

        :param para:
        :return:
        """
        # S0: 准备数据
        daily = self._dataman.select_data(
            para=para.clone().with_comb(DAILY_COMB), index=False
        )
        min5 = self._dataman.select_data(
            para=para.clone().with_comb(MIN5_COMB), index=False
        )
        if dataframe_not_valid(daily) or dataframe_not_valid(min5):
            self._logger.warning_end(para=para)
            return
        # S1: 刷新
        dts = min5[DATETIME]
        _st = pd.to_datetime(dts.iloc[0].date()) + TRADE_TIMES[0]
        _ed = pd.to_datetime(dts.iloc[-1].date()) + TRADE_TIMES[-1]
        # S1.1 处理0及NA值
        _cl1 = self._calendar[
            (self._calendar[DATETIME] >= _st) & (self._calendar[DATETIME] <= _ed)
        ]
        min5 = pd.merge(_cl1, min5, how="left", on=[DATETIME]).reset_index(drop=True)
        min5.loc[min5[CLOSE] == 0, [OPEN, CLOSE, HIGH, LOW, CJL, CJE]] = NAN
        min5[CLOSE] = min5[CLOSE].ffill()
        _f1 = pd.isna(min5[OPEN])
        min5.loc[_f1, [OPEN, HIGH, LOW]] = min5.loc[_f1, CLOSE]
        min5.loc[_f1, [CJL, CJE]] = 0
        # S1.2 处理开盘价/收盘价
        min5[ID] = [x % 48 for x in min5.index]
        min5 = min5.set_index(DATETIME)
        # S1.2.1 处理开盘价
        _o1 = TRADE_TIMES[0]
        _o2 = DataFrame(
            {
                DATETIME: pd.to_datetime(daily[DATE]) + _o1,
                OPEN: daily[OPEN],
            }
        )
        _o3 = _o2.set_index(DATETIME)
        _f2 = min5[ID] == 0
        min5.loc[_f2, OPEN] = _o3
        min5.loc[_f2, HIGH] = np.maxs(min5.loc[_f2, HIGH], min5.loc[_f2, OPEN])
        min5.loc[_f2, LOW] = np.mins(min5.loc[_f2, LOW], min5.loc[_f2, OPEN])
        # S1.3 处理收盘价
        _c1 = TRADE_TIMES[-1]
        _c2 = DataFrame(
            {DATETIME: pd.to_datetime(daily[DATE]) + _c1, CLOSE: daily[CLOSE]}
        )
        _c3 = _c2.set_index(DATETIME)
        _f3 = min5[ID] == 47
        min5.loc[_f3, CLOSE] = _c3
        min5.loc[_f3, HIGH] = np.maxs(min5.loc[_f3, HIGH], min5.loc[_f3, CLOSE])
        min5.loc[_f3, LOW] = np.mins(min5.loc[_f3, LOW], min5.loc[_f3, CLOSE])
        # S1.3 处理最高价/最低价
        min5 = min5.reset_index()
        min5[DATE] = min5[DATETIME].dt.date
        daily, min5 = daily.set_index(DATE), min5.set_index(DATE)
        min5[HIGH_DAILY] = daily[HIGH]
        min5[LOW_DAILY] = daily[LOW]
        min5 = min5.reset_index().set_index(DATETIME)
        # S1.3.1 处理分钟线最高价大于日线最高价（全部处理）
        _f4 = min5[HIGH] > min5[HIGH_DAILY]
        if _f4.any():  # ignore this pycharm warning
            min5.loc[_f4, COLS] = np.mins(
                min5.loc[_f4, COLS], min5.loc[_f4, [HIGH_DAILY]]
            )  # open, ... = min(open, high_daily), ...
        # S1.3.2 处理分钟线最低价小于日线最低价（全部处理）
        _f5 = min5[LOW] < min5[LOW_DAILY]
        if _f5.any():  # ignore this pycharm warning
            min5.loc[_f5, COLS] = np.maxs(
                min5.loc[_f5, COLS], min5.loc[_f5, [LOW_DAILY]]
            )  # open, ... = max(open, low_daily)
        # S1.3.3 处理分钟线最高价小于日线最高价（只处理第一条）
        _hmax = (
            min5.reset_index()
            .sort_values(by=[DATE, HIGH, DATETIME], ascending=[True, False, True])
            .iloc[0::48]
            .set_index(DATETIME)
        )
        _hmax = _hmax[_hmax[HIGH] < _hmax[HIGH_DAILY]]
        min5.loc[_hmax.index, HIGH] = _hmax[HIGH_DAILY]
        # S1.3.4 处理分钟线最低价大于日线最低价（只处理第一条）
        _lmin = (
            min5.reset_index()
            .sort_values(by=[DATE, LOW, DATETIME], ascending=[True, True, True])
            .iloc[0::48]
            .set_index(DATETIME)
        )
        min5.loc[_lmin.index, LOW] = _lmin[LOW_DAILY]
        # S1.4 返回结果
        min5 = min5.reset_index()[[DATETIME, OPEN, CLOSE, HIGH, LOW, CJL, CJE]]
        return min5
