from buffett.analysis.study.pattern import PatternAnalyst
from buffett.analysis.study.stat import StatisticsAnalyst
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.concept import DcConceptDailyHandler
from buffett.download.handler.index import DcIndexDailyHandler
from buffett.download.handler.industry import DcIndustryDailyHandler
from buffett.download.handler.stock import DcDailyHandler
from test import create_1stock, create_1index, create_1industry, create_1concept
from test.analysis.analysis_tester import AnalysisTester


class TestAnalyst(AnalysisTester):
    @classmethod
    def _setup_oncemore(cls):
        create_1stock(operator=cls._operator)
        create_1stock(operator=cls._operator, is_sse=False)
        DcDailyHandler(operator=cls._operator).obtain_data(para=cls._long_para)
        create_1index(operator=cls._operator)
        DcIndexDailyHandler(operator=cls._operator).obtain_data(para=cls._long_para)
        create_1concept(operator=cls._operator)
        DcConceptDailyHandler(operator=cls._operator).obtain_data(para=cls._long_para)
        create_1industry(operator=cls._operator)
        DcIndustryDailyHandler(operator=cls._operator).obtain_data(para=cls._long_para)
        CalendarHandler(operator=cls._operator).obtain_data()

    def _setup_always(self) -> None:
        pass

    def test_pattern(self):
        handler = PatternAnalyst(
            datasource_op=self._select_op, operator=self._insert_op
        )
        handler.calculate(span=self._long_para.span)

    def test_stat(self):
        handler = StatisticsAnalyst(
            datasource_op=self._select_op, operator=self._insert_op
        )
        handler.calculate(span=self._long_para.span)
