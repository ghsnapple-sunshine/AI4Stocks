import multiprocessing
from typing import Union, Optional

from buffett.adapter.numpy import np
from buffett.adapter.pandas import DataFrame, Series, pd
from buffett.adapter.pendulum import DateTime
from buffett.analysis import Para
from buffett.analysis.study.supporter import DataManager
from buffett.analysis.study.tools import get_stock_list
from buffett.analysis.types import CombExType, AnalystType
from buffett.common.constants.col import (
    OPEN,
    CLOSE,
    HIGH,
    LOW,
    CJL,
    CJE,
    DATETIME,
    START_DATE,
    END_DATE,
    TYPE,
)
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.analysis import ANA_MIN5_MTAIN_META
from buffett.common.constants.table import AMA_MIN5_MTAIN
from buffett.common.error import PreStepError
from buffett.common.logger import Logger, LoggerBuilder
from buffett.common.tools import dataframe_not_valid
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.download.types import SourceType, FreqType, FuquanType

HFQ_COMB = CombExType(
    source=SourceType.ANA,
    freq=FreqType.MIN5,
    fuquan=FuquanType.HFQ,
    analysis=AnalystType.CONV,
)
NA, ZERO = 1, 0


class ConvertStockMinuteAnalystMaintain:
    def __init__(self, ana_op: Operator, stk_rop: Operator):
        self._ana_rop = ana_op
        self._ana_wop = ana_op.copy()
        self._stk_rop = stk_rop
        self._dataman = DataManager(ana_rop=self._ana_rop, stk_rop=self._stk_rop)

    def run(self):
        # 准备数据
        stock_list = get_stock_list(operator=self._stk_rop)
        if dataframe_not_valid(stock_list):
            raise PreStepError(
                ConvertStockMinuteAnalystMaintain,
                Union[SseStockListHandler, BsStockListHandler],
            )
        paras = self._create_para(stock_list)
        with multiprocessing.Pool() as workers:
            results = workers.map(self._run_with_subprocess, paras)
        # 保存结果
        self._save_to_database(results)

    def _create_para(
        self, stock_list: DataFrame
    ) -> list[tuple[int, list[str], RoleType, RoleType]]:
        """
        创建多进程计算所需的para

        :param stock_list:
        :return:
        """
        ana_role = self._ana_rop.role
        stk_role = self._stk_rop.role
        stock_list = stock_list[CODE].values.tolist()
        total_num = len(stock_list)
        chunk_num = min(max(total_num, 4) // 4, 8)
        paras = [
            (i, stock_list[i::chunk_num], ana_role, stk_role) for i in range(chunk_num)
        ]
        return paras

    @staticmethod
    def _run_with_subprocess(
        para: tuple[int, list[str], RoleType, RoleType]
    ) -> list[Optional[DataFrame]]:
        """
        分进程运行

        :param para:       stock_list, ana_role, stk_role
        :return:
        """
        tid, stock_list, ana_role, stk_role = para
        worker = ConvertStockMinuteAnalystMaintainWorker(
            ana_rop=Operator(ana_role), stk_rop=Operator(stk_role), tid=tid
        )
        return worker.run(stock_list)

    def _save_to_database(self, results: list[list[Optional[DataFrame]]]):
        """
        将结果保存至数据库

        :return:
        """
        dfs = []
        for r in results:
            dfs.extend(r)
        df = pd.concat(dfs)
        self._ana_wop.drop_table(name=AMA_MIN5_MTAIN)
        self._ana_wop.create_table(name=AMA_MIN5_MTAIN, meta=ANA_MIN5_MTAIN_META)
        self._ana_wop.insert_data_safe(
            name=AMA_MIN5_MTAIN, meta=ANA_MIN5_MTAIN_META, df=df
        )


class ConvertStockMinuteAnalystMaintainWorker:
    """
    分钟线维护的子线程算子
    """

    def __init__(self, ana_rop: Operator, stk_rop: Operator, tid: int):
        self._ana_rop = ana_rop
        self._stk_rop = stk_rop
        self._dataman = DataManager(ana_rop=self._ana_rop, stk_rop=self._stk_rop)
        self._logger = LoggerBuilder.build(StockMinuteAnalystMaintainLogger)(tid=tid)

    def run(self, stock_list: list[str]) -> list[Optional[DataFrame]]:
        """
        运行

        :param stock_list:  股票清单
        :return:
        """
        dfs = [self._calculate_1stock(x) for x in stock_list]
        return dfs

    def _calculate_1stock(self, code: str) -> Optional[DataFrame]:
        """
        判断股票信息里是否有NA及0数据

        :param code:        股票代码
        :return:
        """
        self._logger.info_start(code)
        para = Para().with_code(code).with_comb(HFQ_COMB)
        hfq_info = self._dataman.select_data(para=para, index=False)
        if dataframe_not_valid(hfq_info):
            self._logger.warning_end(code)
            return
        is_na = np.vectorize(pd.isna)
        na_datetimes = hfq_info[
            is_na(hfq_info[OPEN])
            | is_na(hfq_info[CLOSE])
            | is_na(hfq_info[HIGH])
            | is_na(hfq_info[LOW])
            | is_na(hfq_info[CJL])
            | is_na(hfq_info[CJE])
        ][DATETIME]
        na_datetimes_exist = not na_datetimes.empty
        des_na = self._describe(na_datetimes) if na_datetimes_exist else []
        zero_datetimes = hfq_info[
            (hfq_info[OPEN] == 0)
            | (hfq_info[CLOSE] == 0)
            | (hfq_info[HIGH] == 0)
            | (hfq_info[LOW] == 0)
        ][DATETIME]
        zero_datetimes_exist = not zero_datetimes.empty
        des_zero = self._describe(zero_datetimes) if zero_datetimes_exist else []
        if not zero_datetimes_exist and not na_datetimes_exist:
            self._logger.info_success(code)
            return
        df_na, df_zero = None, None
        if na_datetimes_exist:
            self._logger.warning_na_datetimes(code, des_na)
            df_na = DataFrame(des_na, columns=[START_DATE, END_DATE])
            df_na[TYPE] = NA
            df_na[CODE] = code
        if zero_datetimes_exist:
            self._logger.warning_zero_datetimes(code, des_zero)
            df_zero = DataFrame(des_na, columns=[START_DATE, END_DATE])
            df_zero[TYPE] = ZERO
            df_zero[CODE] = code
        return pd.concat([df_na, df_zero])

    @staticmethod
    def _describe(datetimes: Series) -> list[list[DateTime, DateTime]]:
        index = datetimes.index
        _i0 = index[:-1]
        _i1 = index[1:]
        _i2 = _i1 - _i0
        _i3 = _i2 != 1
        start_index = np.concatenate([index[:1], _i1[_i3]])
        end_index = np.concatenate([_i0[_i3], index[-1:]])
        start_datetimes = datetimes[start_index]
        end_datetimes = datetimes[end_index]
        return [[k, v] for k, v in zip(start_datetimes, end_datetimes)]


class StockMinuteAnalystMaintainLogger(Logger):
    def __init__(self, pid: int, total: int):
        self._pid = pid
        self._total = total
        self._curr = 1

    def warning_zero_datetimes(
        self, code: str, des_zero: list[list[DateTime, DateTime]]
    ):
        self.warning(
            f"[P{self._pid}]Zero data found in {code}, regions: {self._convert_des(des_zero)}."
        )

    def warning_na_datetimes(self, code: str, des_na: list[list[DateTime, DateTime]]):
        self.warning(
            f"[P{self._pid}]NA data found in {code}, regions: {self._convert_des(des_na)}."
        )

    def info_start(self, code: str):
        self.info(
            f"[P{self._pid}]({self._curr}/{self._total})Start to maintain {code}."
        )
        self._curr += 1

    def warning_end(self, code: str):
        self.warning(f"[P{self._pid}]End process {code} and nothing found to maintain.")

    def info_success(self, code: str):
        self.info(f"[P{self._pid}]Successfully maintain {code} and no bugs found.")

    @classmethod
    def _convert_des(cls, des: list[list[DateTime, DateTime]]) -> str:
        ls = [f"[{k}, {v}]" for k, v in des]
        return ", ".join(ls)
