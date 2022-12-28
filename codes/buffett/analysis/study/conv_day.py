from typing import Optional

from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst
from buffett.analysis.study.tools import get_stock_list
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import (
    FUQUAN,
    FREQ,
    DATE,
    OPEN,
    CLOSE,
    HIGH,
    LOW,
    CJL,
    CJE,
    ZDF,
    HSL,
    ST,
)
from buffett.common.constants.col.my import USE, DC, BS
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.analysis.definition import CONV_DAILY_META
from buffett.common.constants.meta.handler.definition import DAILY_MTAIN_META
from buffett.common.constants.table import DAILY_MTAIN
from buffett.common.magic import SOURCE
from buffett.common.pendulum import DateSpan
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.download.mysql import Operator
from buffett.download.types import FuquanType, SourceType, FreqType

COLS = [DATE, OPEN, CLOSE, HIGH, LOW, CJL, CJE, ZDF, HSL]


class ConvertStockDailyAnalyst(Analyst):
    def __init__(self, operator: Operator, datasource_op: Operator):
        super(ConvertStockDailyAnalyst, self).__init__(
            datasource_op=datasource_op,
            operator=operator,
            analyst=AnalystType.CONV,
            meta=CONV_DAILY_META,
            use_stock=True,
            use_stock_minute=False,
            use_index=False,
            use_concept=False,
            use_industry=False,
            kwd=DATE,
        )
        self._mtain_result = None

    def calculate(self, span: DateSpan) -> None:
        self._mtain_result = dict(
            (k, v)
            for k, v in self._datasource_op.select_data(
                name=DAILY_MTAIN, meta=DAILY_MTAIN_META
            ).groupby(by=[CODE])
        )
        super(ConvertStockDailyAnalyst, self).calculate(span=span)

    def _get_stock_list(self) -> Optional[DataFrame]:
        """
        获取股票清单

        :return:
        """
        stock_list = get_stock_list(operator=self._datasource_op)
        if dataframe_not_valid(stock_list):
            return
        comb_list = DataFrame(
            {
                SOURCE: [SourceType.ANA] * 2,
                FUQUAN: [FuquanType.BFQ, FuquanType.HFQ],
                FREQ: [FreqType.DAY] * 2,
            }
        )
        stock_list = pd.merge(stock_list, comb_list, how="cross")
        return stock_list

    def _calculate(self, para: Para) -> Optional[DataFrame]:
        """
        执行计算逻辑

        :param para:
        :return:
        """
        mtain_info = self._mtain_result.get(para.target.code)
        if dataframe_not_valid(mtain_info):
            return
        dc_info = self._dataman.select_data(
            para=para.clone().with_source(SourceType.AK_DC), index=False
        )
        bs_info = self._dataman.select_data(
            para=para.clone().with_source(SourceType.BS), index=False
        )
        dc_valid, bs_valid = dataframe_is_valid(dc_info), dataframe_is_valid(bs_info)
        if not dc_valid and not bs_valid:
            return
        all_dates = mtain_info[[DATE]].copy()
        dc_dates = mtain_info[mtain_info[USE] == DC][[DATE]].copy()
        bs_dates = mtain_info[mtain_info[USE] == BS][[DATE]].copy()
        filter_dc_info = None
        if dc_valid:
            filter_dc_info = pd.merge(dc_dates, dc_info, how="left")[COLS]
        filter_bs_info = None
        st = None
        if bs_valid:
            filter_bs_info = pd.merge(bs_dates, bs_info, how="left")[COLS]
            st = pd.merge(all_dates, bs_info, how="left")[[ST]]
        merge_info = pd.concat_safe([filter_bs_info, filter_dc_info])
        if bs_valid:
            merge_info[ST] = st[ST]
        merge_info = merge_info[pd.notna(merge_info[CLOSE])]
        return merge_info
