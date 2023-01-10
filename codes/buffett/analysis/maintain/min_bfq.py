from typing import Union, Optional

from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.analysis.study.supporter import DataManager
from buffett.analysis.study.tools import get_stock_list
from buffett.analysis.types import CombExType, AnalystType
from buffett.common.constants.col import (
    DATE,
    DATETIME,
    OPEN,
    CLOSE,
    HIGH,
    LOW,
    CJL,
    CJE,
)
from buffett.common.constants.col.my import DAILY_SUFFIX, MIN5_SUFFIX
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.analysis.definition import DAILY_MIN5_MTAIN_META
from buffett.common.constants.table import DAILY_MIN5_MTAIN
from buffett.common.error import PreStepError
from buffett.common.interface import MultiProcessTaskManager, ProducerConsumer
from buffett.common.logger import ProgressLogger, LoggerBuilder
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.common.wrapper import Wrapper
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.download.types import SourceType, FreqType, FuquanType

DAILY_COMB = CombExType(
    source=SourceType.ANA,
    freq=FreqType.DAY,
    fuquan=FuquanType.BFQ,
    analysis=AnalystType.CONV,
)
MIN5_COMB = CombExType(
    source=SourceType.ANA,
    freq=FreqType.MIN5,
    fuquan=FuquanType.BFQ,
    analysis=AnalystType.CONV,
)
COLS = [OPEN, CLOSE, HIGH, LOW, CJL, CJE]
COLS_WITH_DATE = [DATE] + COLS
COLS_DAILY = [x + DAILY_SUFFIX for x in COLS[:4]]
COLS_MIN5 = [x + MIN5_SUFFIX for x in COLS[:4]]


class StockMinuteBfqMaintain:
    def __init__(self, ana_rop: Operator, mtain_wop: Operator, stk_rop: Operator):
        self._ana_rop = ana_rop
        self._mtain_wop = mtain_wop
        self._stk_rop = stk_rop

    def run(self):
        """
        运行自检程序

        :return:
        """
        # 准备数据
        stock_list = get_stock_list(operator=self._stk_rop)
        if dataframe_not_valid(stock_list):
            raise PreStepError(
                StockMinuteBfqMaintain,
                Union[SseStockListHandler, BsStockListHandler],
            )
        taskman = MultiProcessTaskManager(
            worker=self._run_with_subprocess,
            args=[
                stock_list[CODE].values.tolist(),
                self._mtain_wop.role,
                self._ana_rop.role,
            ],
        )
        results = taskman.run()
        self._save_to_database(results)

    def _save_to_database(self, dfs: list[Optional[DataFrame]]):
        """
        将维护结果保存至数据库

        :param dfs:
        :return:
        """
        df = pd.concat_safe(dfs)
        if dataframe_is_valid(df):
            self._mtain_wop.create_table(
                name=DAILY_MIN5_MTAIN, meta=DAILY_MIN5_MTAIN_META
            )
            self._mtain_wop.insert_data_safe(
                name=DAILY_MIN5_MTAIN, meta=DAILY_MIN5_MTAIN_META, df=df
            )

    @staticmethod
    def _run_with_subprocess(para: tuple[list[str], RoleType, RoleType]) -> None:
        """
        分进程运行

        :param para:       stock_list, mtain_w, ana_r
        :return:
        """
        stock_list, mtain_w, ana_r = para
        worker = StockDailyMinuteMaintainWorker(
            mtain_wop=Operator(mtain_w), ana_rop=Operator(ana_r)
        )
        worker.run(stock_list)


class StockDailyMinuteMaintainWorker:
    """
    日线分钟线对比维护的子线程算子
    """

    def __init__(self, mtain_wop: Operator, ana_rop: Operator):
        self._mtain_wop = mtain_wop
        self._ana_rop = ana_rop
        self._dataman = DataManager(ana_rop=ana_rop, stk_rop=None)
        self._logger = None

    def run(self, stock_list: list[str]) -> None:
        """
        运行

        :param stock_list:  股票清单
        :return:
        """
        # 准备工作区
        self._logger = LoggerBuilder.build(StockDailyMinuteMaintainLogger)(
            total=len(stock_list)
        )
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._get_data_for_1stock),
            consumer=Wrapper(self._calculate_1stock),
            args_map=stock_list,
            queue_size=3,
            task_num=len(stock_list),
        )
        prod_cons.run()

    def _get_data_for_1stock(self, code: str) -> tuple[str, DataFrame, DataFrame]:
        """
        根据股票代码获取分钟线数据

        :param code:        股票代码
        :return:            股票代码, 分钟线数据, 日线数据
        """
        self._logger.info_start(code)
        daily = self._dataman.select_data(
            para=Para().with_code(code).with_comb(DAILY_COMB), index=False
        )
        min5 = self._dataman.select_data(
            para=Para().with_code(code).with_comb(MIN5_COMB), index=False
        )
        return code, daily, min5

    def _calculate_1stock(
        self, info: tuple[str, DataFrame, DataFrame]
    ) -> Optional[DataFrame]:
        """
        判断股票信息里是否有NA及0数据

        :param info:        股票代码, 日线数据, 分钟线数据
        :return:
        """

        code, daily, min5 = info
        if dataframe_not_valid(daily) or dataframe_not_valid(min5):
            self._logger.warning_end(code)
            return

        min5[DATE] = min5[DATETIME].dt.date
        min5_groupby = (
            min5.groupby(by=[DATE]).apply(self._groupby_date).reset_index(drop=True)
        )
        merge = pd.merge(
            min5_groupby,
            daily[COLS_WITH_DATE],
            how="left",
            on=[DATE],
            suffixes=[MIN5_SUFFIX, DAILY_SUFFIX],
        )
        compares = [merge[x] != merge[y] for x, y in zip(COLS_DAILY, COLS_MIN5)]
        compare = compares[0]
        for x in compares[1:]:
            compare = compare | x
        merge_filter = merge[compare].copy()
        if dataframe_is_valid(merge_filter):
            merge_filter[CODE] = code
            self._logger.warning_diff_exists(code)
            return merge_filter
        self._logger.info_success(code)

    @staticmethod
    def _groupby_date(df: DataFrame) -> DataFrame:
        date = df[DATE].iloc[0]
        open = df[OPEN].iloc[0]
        close = df[CLOSE].iloc[-1]
        high = df[HIGH].max()
        low = df[LOW].min()
        cjl = df[CJL].sum()
        cje = df[CJE].sum()
        return DataFrame(
            [[date, open, close, high, low, cjl, cje]],
            columns=COLS_WITH_DATE,
        )


class StockDailyMinuteMaintainLogger(ProgressLogger):
    def warning_diff_exists(self, code: str):
        self.warning(f"End process {code} and diff exists.")

    def info_start(self, code: str):
        self.info_with_progress(f"Start to maintain {code}.", update=True)

    def warning_end(self, code: str):
        self.warning(f"End process {code} and nothing found to maintain.")

    def info_success(self, code: str):
        self.info(f"Successfully maintain {code} and no bugs found.")
