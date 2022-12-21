from buffett.adapter.numpy import NAN
from buffett.adapter.pandas import pd, DataFrame
from buffett.adapter.pendulum import Date
from buffett.common.constants.col import CJL, CLOSE, DATE, START_DATE, END_DATE, SOURCE
from buffett.common.constants.col.target import CODE
from buffett.common.error import PreStepError
from buffett.common.interface import ProducerConsumer
from buffett.common.logger import Logger, LoggerBuilder
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.common.wrapper import Wrapper
from buffett.download import Para
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.stock import BsDailyHandler, DcDailyHandler
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import CombType, SourceType, FreqType, FuquanType
from buffett.download.maintain.base import BaseMaintain

BS_COMB = CombType(source=SourceType.BS, freq=FreqType.DAY, fuquan=FuquanType.BFQ)
DC_COMB = CombType(source=SourceType.AK_DC, freq=FreqType.DAY, fuquan=FuquanType.BFQ)
CLOSEr = CLOSE + "_r"
BS, DC = "bs", "dc"
ID, SPAN = "id", "span"


class StockDailyMaintain(BaseMaintain):
    def __init__(self, operator: Operator):
        super(StockDailyMaintain, self).__init__(folder="daily_maintain")
        self._operator = operator
        self._dl_recorder = DownloadRecorder(operator=operator)
        self._bs_handler = BsDailyHandler(operator=operator)
        self._dc_handler = DcDailyHandler(operator=operator)
        self._calendar_handler = CalendarHandler(operator=operator)
        self._check_results = None
        self._calendar = None
        self._logger = LoggerBuilder.build(StockDailyMaintainLogger)()

    def run(self) -> bool:
        """
        检查股票的baostock, dongcai的日线数据是否匹配

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
            return True
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
        self._save_report(df=data)
        return dataframe_not_valid(data)

    def _get_data_for_1stock(self, code: str) -> tuple[str, DataFrame, DataFrame]:
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
        return code, bs_info, dc_info

    def _check_1stock(self, infos: tuple[str, DataFrame, DataFrame]) -> None:
        """
        检查某支股票的baostock, dongcai的日线数据是否匹配

        :param infos:        code, bs_info, dc_info
        :return:
        """
        code, bs_info, dc_info = infos
        bs_valid, dc_valid = dataframe_is_valid(bs_info), dataframe_is_valid(dc_info)
        bs_miss, dc_miss = None, None
        if bs_valid:
            bs_info = bs_info[bs_info[CJL] > 0]  # 过滤停牌日
        if dc_valid:
            dc_info = dc_info[dc_info.index < Date.today()]
        if bs_valid and dc_valid:
            merge_info = pd.merge(
                bs_info,
                dc_info,
                how="outer",
                left_index=True,
                right_index=True,
                suffixes=["", "_r"],
            )
            bs_miss = merge_info[pd.isna(merge_info[CLOSE])][[CLOSEr]]
            dc_miss = merge_info[pd.isna(merge_info[CLOSEr])][[CLOSE]]
        elif not bs_valid and dc_valid:
            bs_miss = dc_info[[CLOSE]]
        elif bs_valid and not dc_valid:
            dc_miss = bs_info[[CLOSE]]
        if dataframe_is_valid(bs_miss) or dataframe_is_valid(dc_miss):
            self._check_results.append(
                DataFrame(
                    data=[[code, self._describe(bs_miss), self._describe(dc_miss)]],
                    columns=[CODE, BS, DC],
                )
            )
            self._logger.info_maintain_end(code=code, success=False)
        else:
            self._logger.info_maintain_end(code=code, success=True)

    def _describe(self, dates) -> str:
        """
        将dates拆分成若干个可被描述的区间，

        :param dates:
        :return:
        """
        if dataframe_not_valid(dates):
            return ""
        merge = pd.merge(self._calendar, dates, how="left", on=[DATE])
        merge.loc[pd.notna(merge.iloc[:, -1]), ID] = NAN
        merge = merge.fillna(method="ffill")
        dates = pd.merge(dates, merge, how="left", on=[DATE])
        dates = DataFrame(
            {START_DATE: dates.index, END_DATE: dates.index, ID: dates[ID]}
        )
        groupby_dates = dates.groupby(by=[ID]).aggregate(
            {START_DATE: "min", END_DATE: "max"}
        )
        groupby_dates[SPAN] = groupby_dates.apply(
            lambda x: f"[{x[START_DATE]},{x[END_DATE]}]", axis=1
        )
        return ", ".join(groupby_dates[SPAN].tolist())


class StockDailyMaintainLogger(Logger):
    @classmethod
    def info_maintain_start(cls, code: str):
        Logger.info(f"Start maintain daily download for {code}.")

    @classmethod
    def info_maintain_end(cls, code: str, success: bool):
        supp = "" if success else ", diff exists"
        Logger.info(f"Successfully maintain daily download for {code}{supp}.")
