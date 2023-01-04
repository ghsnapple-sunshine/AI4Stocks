from buffett.adapter.pendulum import Date
from buffett.analysis import Para
from buffett.analysis.study import ConvertStockDailyAnalyst
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import DATE
from buffett.common.constants.col.my import USE, DC, BS
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.handler import DAILY_MTAIN_META
from buffett.common.constants.table import DAILY_MTAIN
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.stock import DcDailyHandler, BsDailyHandler
from buffett.download.types import SourceType, FreqType, FuquanType
from test import create_1stock
from test.analysis.analysis_tester import AnalysisTester


class TestConvertStockDailyAnalyst(AnalysisTester):
    _calendar = None
    _dc_handler = None
    _bs_handler = None
    _analyst = None

    @classmethod
    def _setup_oncemore(cls):
        create_1stock(operator=cls._operator, source="both")
        cls._create_mtain_result()
        cls._dc_handler = DcDailyHandler(operator=cls._operator)
        cls._dc_handler.obtain_data(para=cls._short_para)
        cls._bs_handler = BsDailyHandler(operator=cls._operator)
        cls._bs_handler.obtain_data(para=cls._short_para)
        cls._analyst = ConvertStockDailyAnalyst(
            stk_rop=cls._datasource_op, ana_rop=cls._operator
        )

    @classmethod
    def _create_mtain_result(cls):
        calendarman = CalendarHandler(operator=cls._operator)
        calendarman.obtain_data()
        cls._calendar = calendarman.select_data(para=cls._short_para).reset_index()
        cls._calendar[CODE] = "000001"
        cls._calendar[USE] = DC
        cls._calendar.loc[cls._calendar[DATE] >= Date(2020, 3, 1), USE] = BS
        cls._operator.create_table(name=DAILY_MTAIN, meta=DAILY_MTAIN_META)
        cls._operator.insert_data_safe(
            name=DAILY_MTAIN, meta=DAILY_MTAIN_META, df=cls._calendar
        )

    def _setup_always(self) -> None:
        pass

    def test_calculate(self):
        self._analyst.calculate(span=self._short_para.span)
        bfq_para = (
            Para()
            .with_code("000001")
            .with_source(SourceType.ANA)
            .with_freq(FreqType.DAY)
            .with_fuquan(FuquanType.BFQ)
            .with_analysis(AnalystType.CONV)
        )
        hfq_para = bfq_para.clone().with_fuquan(FuquanType.HFQ)
        bfq_data = self._analyst.select_data(para=bfq_para)
        hfq_data = self._analyst.select_data(para=hfq_para)
        assert len(bfq_data) == len(hfq_data) == len(self._calendar)
