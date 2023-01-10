from __future__ import annotations

from typing import Optional

from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst, AnalystWorker
from buffett.analysis.study.base.analyst import AnalysisLogger
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
from buffett.common.constants.col.my import USE, DC
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.analysis import CONV_DAILY_META
from buffett.common.interface import MultiProcessTaskManager, ProducerConsumer
from buffett.common.logger import LoggerBuilder
from buffett.common.magic import SOURCE
from buffett.common.pendulum import DateSpan
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.common.wrapper import Wrapper
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.maintain import StockDailyMaintain
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.download.types import FuquanType, SourceType, FreqType

COLS = [DATE, OPEN, CLOSE, HIGH, LOW, CJL, CJE, ZDF, HSL]


class ConvertStockDailyAnalyst(Analyst):
    def __init__(self, ana_rop: Operator, ana_wop: Operator, stk_rop: Operator):
        super(ConvertStockDailyAnalyst, self).__init__(
            stk_rop=stk_rop,
            ana_rop=ana_rop,
            ana_wop=ana_wop,
            analyst=AnalystType.CONV,
            meta=CONV_DAILY_META,
            Worker=ConvertStockDailyAnalystWorker,
            use_stock=True,
            use_stock_minute=False,
            use_index=False,
            use_concept=False,
            use_industry=False,
        )

    def calculate(self, span: DateSpan) -> None:
        """
        在指定的时间范围内计算某类型数据

        :param span:
        :return:
        """
        todo_records = self._get_todo_records(span=span)
        done_records = self._recorder.select_data()
        comb_records = self._get_comb_records(
            todo_records=todo_records, done_records=done_records
        )
        mtain_ls = self._get_mtain_info(comb_records=comb_records)
        taskman = MultiProcessTaskManager(
            worker=self._run_with_multiprocess,
            args=[
                comb_records,
                mtain_ls,
                self._ana_rop.role,
                self._ana_wop.role,
                self._stk_rop.role,
                self._analyst,
                self._meta,
            ],
            iterable_args=2,
        )
        taskman.run()

    @staticmethod
    def _run_with_multiprocess(
        para: tuple[
            DataFrame,
            list[list[str]],
            RoleType,
            RoleType,
            RoleType,
            AnalystType,
            DataFrame,
        ]
    ) -> None:
        """
        基于子进程运行

        :param para:        comb_records, dc_dates, ana_r, ana_w, stk_r, analysis, meta
        :return:
        """
        (
            comb_records,
            dc_dates,
            ana_r,
            ana_w,
            stk_r,
            analyst,
            meta,
        ) = para
        worker = ConvertStockDailyAnalystWorker(
            ana_rop=Operator(ana_r),
            ana_wop=Operator(ana_w),
            stk_rop=Operator(stk_r),
            analyst=analyst,
            meta=meta,
        )
        worker.calculate_ex(comb_records, dc_dates)

    def _get_stock_list(self) -> Optional[DataFrame]:
        """
        获取股票清单

        :return:
        """
        stock_list = get_stock_list(operator=self._stk_rop)
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

    def _get_mtain_info(self, comb_records):
        """
        准备daily maintain结果并瘦身

        :param comb_records:
        :return:
        """
        mtain_info = StockDailyMaintain(operator=self._stk_rop).select_data()
        filter_info = mtain_info[mtain_info[USE] == DC]
        mtain_dict = dict(
            (k, v[DATE].tolist()) for k, v in filter_info.groupby(by=[CODE])
        )
        mtain_ls = [mtain_dict.get(x) for x in comb_records[CODE]]
        return mtain_ls


class ConvertStockDailyAnalystWorker(AnalystWorker):
    def __init__(
        self,
        ana_rop: Operator,
        ana_wop: Operator,
        stk_rop: Operator,
        analyst: AnalystType,
        meta: DataFrame,
    ):
        super(ConvertStockDailyAnalystWorker, self).__init__(
            stk_rop=stk_rop,
            ana_rop=ana_rop,
            ana_wop=ana_wop,
            analyst=analyst,
            meta=meta,
        )
        self._calendar = CalendarHandler(operator=stk_rop).select_data(
            index=False, to_datetimes=False
        )
        self._dc_dates = None

    def calculate_ex(self, comb_records: DataFrame, dc_dates: list[list[str]]):
        """
        Worker启动计算

        :param comb_records:
        :param dc_dates:
        :return:
        """
        # 初始化Logger
        self._logger = LoggerBuilder.build(AnalysisLogger)(
            analyst=self._analyst, total=len(comb_records)
        )
        self._dc_dates = dict(zip(comb_records[CODE], dc_dates))
        # 使用生产者/消费者模式，异步下载/保存数据
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._calculate_1target),
            consumer=Wrapper(self._save_1target),
            queue_size=3,
            args_map=comb_records.itertuples(index=False),
            task_num=len(comb_records),
        )
        prod_cons.run()

    def _calculate(self, para: Para) -> Optional[DataFrame]:
        """
        执行计算逻辑

        :param para:
        :return:
        """
        # S0: 准备数据
        dc_dates, dc_info, bs_info = self._prepare(para)
        bs_valid, dc_valid = dataframe_is_valid(bs_info), dataframe_is_valid(dc_info)
        if not bs_valid and not dc_valid:
            self._logger.warning_end(para=para)
            return
        # S1: 计算
        # S1.1: 计算st, ed
        if bs_valid and dc_valid:
            _st = min(bs_info[DATE].iloc[0], dc_info[DATE].iloc[0])
            _ed = max(bs_info[DATE].iloc[-1], dc_info[DATE].iloc[-1])
        elif bs_valid:  # and not dc_valid:
            _st, _ed = bs_info[DATE].iloc[0], bs_info[DATE].iloc[-1]
        else:
            _st, _ed = dc_info[DATE].iloc[0], dc_info[DATE].iloc[-1]
        _c1 = self._calendar[
            (self._calendar[DATE] >= _st) & (self._calendar[DATE] <= _ed)
        ]
        # S1.2: 处理dc部分
        if dc_valid:
            # 筛选出使用DC数据的日期，把指定列关联到总表上
            _d1 = pd.merge(dc_dates, dc_info[COLS], how="left", on=[DATE])
            _d1[CJL] = _d1[CJL] * 100
            _c2 = pd.merge(_c1, _d1, how="left", on=[DATE])
        else:
            _c2 = None
        # S1.3: 处理bs部分
        if bs_valid:
            # 筛选出未使用DC数据的日期，把指定列关联到总表上
            if _c2 is None:
                _c3 = pd.merge(_c1, bs_info[COLS], how="left", on=[DATE])
            else:
                _b0 = _c2[pd.isna(_c2[CLOSE])][[DATE]]
                _b1 = pd.merge(_b0, bs_info[COLS], how="left", on=[DATE])
                _b2 = _c2[pd.notna(_c2[CLOSE])]
                _c3 = pd.concat([_b1, _b2]).sort_values(by=[DATE])
            # ST列全部使用BS数据源
            _c4 = pd.merge(_c3, bs_info[[DATE, ST]], how="left", on=[DATE])
        else:
            _c4 = _c2
        _c4[CLOSE] = _c4[CLOSE].ffill()
        _f1 = pd.isna(_c4[OPEN])
        _c4.loc[_f1, [OPEN, HIGH, LOW]] = _c4.loc[_f1, CLOSE]
        _c4.loc[_f1, [CJL, CJE, ZDF, HSL]] = 0
        # 剔除开盘日之前的数据
        conv_info = _c4[pd.notna(_c4[CLOSE])]
        return conv_info

    def _prepare(
        self, para: Para
    ) -> tuple[Optional[DataFrame], Optional[DataFrame], Optional[DataFrame]]:
        """
        准备数据

        :param para:
        :return:
        """
        dc_dates = self._dc_dates.get(para.target.code)
        dc_dates = [] if dc_dates is None else dc_dates
        dc_info = self._dataman.select_data(
            para=para.clone().with_source(SourceType.AK_DC), index=False
        )
        bs_info = self._dataman.select_data(
            para=para.clone().with_source(SourceType.BS), index=False
        )
        return DataFrame({DATE: dc_dates}), dc_info, bs_info
