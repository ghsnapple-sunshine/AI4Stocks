from buffett.adapter.numpy import np
from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis.study.fuquan import FuquanAnalyst, FuquanAnalystLogger
from buffett.common.constants.col import DATE, CLOSE, A, START_DATE, END_DATE, B
from buffett.common.constants.meta.analysis import FQ_FAC_META
from buffett.common.constants.table import FQ_FAC_V2
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.download.mysql import Operator


class FuquanAnalystV2(FuquanAnalyst):
    def __init__(self, ana_rop: Operator, ana_wop: Operator, stk_rop: Operator):
        super(FuquanAnalystV2, self).__init__(
            ana_rop=ana_rop, ana_wop=ana_wop, stk_rop=stk_rop
        )
        # override parameters
        self._NAME = FQ_FAC_V2
        self._META = FQ_FAC_META

    def _calc_1stock_with_fhpg(
        self, info: tuple[str, DataFrame, DataFrame, DataFrame]
    ) -> None:
        """
        计算携带有分红跑配股信息的一支股票

        :param info:        股票代码, 分红配股信息, 不复权信息, 后复权信息
        :return:            拟合表
        """
        code, fhpg_info, bfq_info, hfq_info = info
        # 过滤fhpg_info
        if dataframe_not_valid(bfq_info):
            self._logger.warning_calculate_end(code)
            return
        _st, _ed = bfq_info[DATE].iloc[0], bfq_info[DATE].iloc[-1]
        # 将除权日与数据起始日连接，作为区间起点
        # 股票可能一天有两条记录（e.g.派息+增发），数据起始日也可能与除权日重叠，因此需要去重
        # 指定的span可能大于不复权和后复权数据的区间，因此需要筛选
        _f0 = bfq_info[[DATE]]
        _f1 = pd.concat(
            [
                DataFrame({DATE: [_st]}),
                fhpg_info[(fhpg_info[DATE] >= _st) & (fhpg_info[DATE] <= _ed)][[DATE]],
            ]
        ).drop_duplicates()
        _f2 = _f1.rename(columns={DATE: START_DATE})
        _d0 = bfq_info[DATE].values
        _d1 = DataFrame(
            {
                START_DATE: _d0,
                END_DATE: np.concatenate([_d0[1:], _d0[-1:]]),
                A: hfq_info[CLOSE] / bfq_info[CLOSE],
                B: 0,
            }
        )
        # 除权日可能不是交易日
        _d2 = pd.merge(_f2, _d1, how="outer", on=[START_DATE]).sort_values(
            by=[START_DATE]
        )
        _d3 = _d2.fillna(method="ffill")
        merge_info = pd.merge(_f2, _d3, how="left", on=[START_DATE])
        self._factors[code] = merge_info
        if dataframe_is_valid(merge_info):
            self._logger.info_end_calculate(code)
        else:
            self._logger.warning_calculate_error(code)


class FuquanAnalystLoggerV2(FuquanAnalystLogger):
    @classmethod
    def warning_calculate_error(cls, code: str):
        cls.warning(
            f"End calculate Fuquan Factor for {code}, nothing is found for save."
        )
