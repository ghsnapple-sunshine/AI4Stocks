from buffett.analysis import Para
from buffett.analysis.study import FuquanAnalyst
from buffett.analysis.study.conv_min import ConvertStockMinuteAnalyst
from buffett.analysis.types import AnalystType
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.stock import BsMinuteHandler, DcDailyHandler
from buffett.download.handler.stock.dc_fhpg import DcFhpgHandler
from buffett.download.types import SourceType, FreqType, FuquanType
from test import create_1stock
from test.analysis.analysis_tester import AnalysisTester


class TestStockConvertMinuteAnalyst(AnalysisTester):
    _daily_handler = None
    _min5_handler = None
    _fuquan_analyst = None
    _fhpg_handler = None
    _calendar_handler = None

    @classmethod
    def _setup_oncemore(cls):
        create_1stock(operator=cls._operator, source="both")
        cls._daily_handler = DcDailyHandler(operator=cls._operator)
        cls._daily_handler.obtain_data(para=cls._long_para)
        cls._min5_handler = BsMinuteHandler(operator=cls._operator)
        cls._min5_handler.obtain_data(para=cls._long_para)
        cls._fhpg_handler = DcFhpgHandler(operator=cls._operator)
        cls._fhpg_handler.obtain_data()
        cls._calendar_handler = CalendarHandler(operator=cls._operator)
        cls._calendar_handler.obtain_data()
        cls._fuquan_analyst = FuquanAnalyst(
            operator=cls._insert_op, datasource_op=cls._select_op
        )
        cls._fuquan_analyst.calculate(span=cls._long_para.span)
        cls._conv_analyst = ConvertStockMinuteAnalyst(
            operator=cls._insert_op, datasource_op=cls._select_op
        )

    def _setup_always(self) -> None:
        pass

    def test_calculate(self):
        row = self._min5_handler.select_data(para=self._long_para).shape[0]
        self._conv_analyst.calculate(span=self._long_para.span)
        row2 = self._conv_analyst.select_data(
            para=Para()
            .with_code("000001")
            .with_analysis(AnalystType.CONV)
            .with_source(SourceType.ANA)
            .with_freq(FreqType.MIN5)
            .with_fuquan(FuquanType.HFQ)
        ).shape[0]
        assert row == row2
