from typing import Optional

from buffett.adapter.numpy import np
from buffett.adapter.pandas import pd, DataFrame
from buffett.adapter.pendulum import Date
from buffett.common.constants.col import (
    CLOSE,
    SOURCE,
    TP,
    CJE,
    OPEN,
    HIGH,
    LOW,
    CJL,
    DATE,
)
from buffett.common.constants.col.my import USE, BS, DC
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.handler.definition import DAILY_MTAIN_META
from buffett.common.constants.table import DAILY_MTAIN
from buffett.common.error import PreStepError
from buffett.common.interface import ProducerConsumer
from buffett.common.logger import Logger, LoggerBuilder
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.common.wrapper import Wrapper
from buffett.download import Para
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.stock import (
    BsDailyHandler,
    DcDailyHandler,
    ThDailyHandler,
)
from buffett.download.maintain.base import BaseMaintain
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import CombType, SourceType, FreqType, FuquanType

#
BS_COMB = CombType(source=SourceType.BS, freq=FreqType.DAY, fuquan=FuquanType.BFQ)
DC_COMB = CombType(source=SourceType.AK_DC, freq=FreqType.DAY, fuquan=FuquanType.BFQ)
TH_COMB = CombType(source=SourceType.AK_TH, freq=FreqType.DAY, fuquan=FuquanType.BFQ)
#
FIELDS = [HIGH, LOW, OPEN, CLOSE]
HIGHb, LOWb, OPENb, CLOSEb = FIELDSb = [x + "_b" for x in FIELDS]
HIGHd, LOWd, OPENd, CLOSEd = FIELDSd = [x + "_d" for x in FIELDS]
HIGHt, LOWt, OPENt, CLOSEt = FIELDSt = [x + "_t" for x in FIELDS]
ALL_FIELDS = np.array([FIELDSb, FIELDSd, FIELDSt], dtype=object)
#
RENAMEb = dict((FIELDS[x], FIELDSb[x]) for x in range(4))
RENAMEd = dict((FIELDS[x], FIELDSd[x]) for x in range(4))
RENAMEt = dict((FIELDS[x], FIELDSt[x]) for x in range(4))
#

BS_DC, DC_TH, TH_BS = "bs_dc_err", "dc_th_err", "th_bs_err"
MIN = "min_err"
UNC = -1
#
ID = "id"


class StockDailyMaintain(BaseMaintain):
    def __init__(self, operator: Operator):
        super(StockDailyMaintain, self).__init__(folder="daily_maintain")
        self._operator = operator
        self._dl_recorder = DownloadRecorder(operator=operator)
        self._bs_handler = BsDailyHandler(operator=operator)
        self._dc_handler = DcDailyHandler(operator=operator)
        self._th_handler = ThDailyHandler(
            operator=operator, target_list_handler=None
        )  # TODO: 待优化
        self._calendar_handler = CalendarHandler(operator=operator)
        self._check_results = None
        self._calendar = None
        self._logger = LoggerBuilder.build(StockDailyMaintainLogger)()

    def run(self, save: bool = True) -> Optional[DataFrame]:
        """
        检查股票的baostock, dongcai, tonghuashun的日线数据是否匹配

        :param save:       保存结果至磁盘
        :return:
        """
        # 准备工作空间
        self._check_results = []
        self._calendar = self._calendar_handler.select_data()
        if dataframe_not_valid(self._calendar):
            raise PreStepError(StockDailyMaintain, CalendarHandler)
        self._calendar[ID] = range(len(self._calendar))
        #
        dl_records = self._dl_recorder.select_data()
        if dataframe_not_valid(dl_records):
            return
        codes = dl_records[
            (dl_records[SOURCE] == SourceType.BS)
            | (dl_records[SOURCE] == SourceType.AK_DC)
        ][CODE].drop_duplicates()
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._get_data_for_1stock),
            consumer=Wrapper(self._check_1stock),
            args_map=codes,
            queue_size=30,
            task_num=len(codes),
        )
        prod_cons.run()
        data = pd.concat_safe(self._check_results)
        data = self._determine_use(data)
        if save:
            self._save_report(df=data, feather=True, csv=True)
            self._save_to_database(df=data)
        else:
            return data

    def _get_data_for_1stock(
        self, code: str
    ) -> tuple[str, DataFrame, DataFrame, DataFrame]:
        """
        分别获取baostock和dongcai的某支股票的数据

        :param code:
        :return:
        """
        self._logger.info_maintain_start(code)
        bs_info = self._bs_handler.select_data(
            para=Para().with_code(code).with_comb(BS_COMB)
        )
        dc_info = self._dc_handler.select_data(
            para=Para().with_code(code).with_comb(DC_COMB)
        )
        th_info = self._th_handler.select_data(
            para=Para().with_code(code).with_comb(TH_COMB)
        )
        return code, bs_info, dc_info, th_info

    def _check_1stock(self, infos: tuple[str, DataFrame, DataFrame, DataFrame]) -> None:
        """
        检查某支股票的baostock, dongcai的日线数据是否匹配

        :param infos:        code, bs_info, dc_info
        :return:
        """
        code, bs_info, dc_info, th_info = infos
        bs_valid, dc_valid, th_valid = valids = [
            dataframe_is_valid(x) for x in infos[1:]
        ]
        sum_valids = sum(valids)
        if bs_valid:
            # 过滤停牌日和成交量（额）为0的日期
            # bs数据中有成交量不为0但成交额为0，疑似bug导致的脏数据
            bs_info = bs_info[
                (bs_info[TP] > 0) & (bs_info[CJL] > 0) & (bs_info[CJE] > 0)
            ]
            bs_info = bs_info.rename(columns=RENAMEb)[FIELDSb]
        if dc_valid:
            dc_info = dc_info[dc_info.index < Date.today()]
            dc_info = dc_info.rename(columns=RENAMEd)[FIELDSd]
        if th_valid:
            th_info = th_info[(th_info[CJL] > 0)]
            th_info = th_info.rename(columns=RENAMEt)[FIELDSt]
        infos = bs_info, dc_info, th_info
        if sum_valids == 0:
            merge_info = None
        elif sum_valids == 1:
            merge_info = np.array(infos, dtype=object)[valids][0]
        elif sum_valids == 2:
            _m0, _m1 = np.array(infos, dtype=object)[valids]
            _f0, _f1 = ALL_FIELDS[valids]
            merge_info = pd.merge(
                _m0, _m1, how="outer", left_index=True, right_index=True
            )
        else:
            merge_info = pd.merge(
                bs_info, dc_info, how="outer", left_index=True, right_index=True
            )
            merge_info = pd.merge(
                merge_info, th_info, how="outer", left_index=True, right_index=True
            )
        if dataframe_is_valid(merge_info):
            merge_info = merge_info.reset_index()
            merge_info[CODE] = code
            self._check_results.append(merge_info)
        self._logger.info_maintain_end(code=code)

    @staticmethod
    def _determine_use(data: DataFrame):
        def compare(dat, filed1, filed2):
            return (
                (dat[filed1[0]] == dat[filed2[0]])
                & (dat[filed1[1]] == dat[filed2[1]])
                & (dat[filed1[2]] == dat[filed2[2]])
                & (dat[filed1[3]] == dat[filed2[3]])
            )

        def abs_error(dat, field1, field2):
            return (
                np.abs(dat[field1[0]] - dat[field2[0]])
                + np.abs(dat[field1[1]] - dat[field2[1]])
                + np.abs(dat[field1[2]] - dat[field2[2]])
                + np.abs(dat[field1[3]] - dat[field2[3]])
            )

        data[USE] = UNC
        na_d, na_b, na_t = (
            pd.isna(data[CLOSEd]),
            pd.isna(data[CLOSEb]),
            pd.isna(data[CLOSEt]),
        )
        nna_d, nna_b, nna_t = (
            pd.notna(data[CLOSEd]),
            pd.notna(data[CLOSEb]),
            pd.notna(data[CLOSEt]),
        )
        data.loc[nna_d & na_b & na_t, USE] = DC
        data.loc[na_d & nna_b & na_t, USE] = BS
        data.loc[na_d & na_b & nna_t, USE] = DC
        # 1 data.loc[na_d & nna_b & nna_t & compare(FIELDSb, FIELDSt), USE] = BS
        # 2 data.loc[nna_d & nna_b & na_t & compare(FIELDSd, FIELDSb), USE] = DC
        # 3 data.loc[nna_d & na_b & nna_t & compare(FIELDSd, FIELDSt), USE] = DC
        # 4 data.loc[na_d & nna_b & nna_t & compare(FIELDSb, FIELDSt), USE] = BS
        # 5 data.loc[nna_d & nna_b & na_t & compare(FIELDSd, FIELDSb), USE] = DC
        # 6 data.loc[nna_d & na_b & nna_t & compare(FIELDSd, FIELDSt), USE] = DC
        # 1+4必须先执行，因为nna_d & nna_b & nna_t的情况下结果应该是DC
        data.loc[nna_b & nna_t & compare(data, FIELDSb, FIELDSt), USE] = BS  # 1+4
        data.loc[nna_d & nna_b & compare(data, FIELDSd, FIELDSb), USE] = DC  # 2+5
        data.loc[nna_d & nna_t & compare(data, FIELDSd, FIELDSt), USE] = DC  # 3+6
        # 处理仍然是unknown的数据
        unknown = data[data[USE] == UNC].copy()
        if dataframe_is_valid(unknown):
            unknown.loc[pd.isna(unknown[CLOSEt]), USE] = DC
            unknown.loc[pd.isna(unknown[CLOSEb]), USE] = DC
            unknown.loc[pd.isna(unknown[CLOSEd]), USE] = BS
            unknown[BS_DC] = abs_error(unknown, FIELDSb, FIELDSd)
            unknown[DC_TH] = abs_error(unknown, FIELDSd, FIELDSt)
            unknown[TH_BS] = abs_error(unknown, FIELDSt, FIELDSb)
            unknown[MIN] = np.vectorize(min)(
                unknown[BS_DC], unknown[DC_TH], unknown[TH_BS]
            )
            unknown.loc[unknown[BS_DC] == unknown[MIN]] = DC
            unknown.loc[unknown[DC_TH] == unknown[MIN]] = DC
            unknown.loc[unknown[TH_BS] == unknown[MIN]] = BS
            data.loc[data[USE] == UNC, USE] = unknown.loc[:, USE]
        return data

    def _save_to_database(self, df: DataFrame):
        """
        将文件保存至数据库

        :param df:
        :return:
        """
        self._operator.drop_table(name=DAILY_MTAIN)
        self._operator.create_table(name=DAILY_MTAIN, meta=DAILY_MTAIN_META)
        df = df[[DATE, CODE, USE]]
        self._operator.insert_data_safe(name=DAILY_MTAIN, meta=DAILY_MTAIN_META, df=df)


class StockDailyMaintainLogger(Logger):
    @classmethod
    def info_maintain_start(cls, code: str):
        Logger.info(f"Start maintain daily download for {code}.")

    @classmethod
    def info_maintain_end(cls, code: str):
        Logger.info(f"Successfully maintain daily download for {code}.")
