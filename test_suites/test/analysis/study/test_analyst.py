from buffett.analysis import Para
from buffett.analysis.study import PatternAnalyst, StatZdfAnalyst, FuquanAnalyst
from buffett.analysis.types import AnalystType
from buffett.common.tools import dataframe_is_valid
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.concept import DcConceptDailyHandler
from buffett.download.handler.index import DcIndexDailyHandler
from buffett.download.handler.industry import DcIndustryDailyHandler
from buffett.download.handler.stock import DcDailyHandler
from buffett.download.types import SourceType, FreqType, FuquanType, CombType
from test import create_1stock, create_1index, create_1industry, create_1concept
from test.analysis.analysis_tester import AnalysisTester


class TestAnalyst(AnalysisTester):
    """
    测试所有的Analyst
    """

    @classmethod
    def _setup_oncemore(cls):
        create_1stock(operator=cls._operator, source="both")
        cls._daily_handler = DcDailyHandler(operator=cls._operator).obtain_data(
            para=cls._long_para
        )
        create_1index(operator=cls._operator)
        cls._index_handler = DcIndexDailyHandler(operator=cls._operator).obtain_data(
            para=cls._long_para
        )
        create_1concept(operator=cls._operator)
        cls._concept_handler = DcConceptDailyHandler(
            operator=cls._operator
        ).obtain_data(para=cls._long_para)
        create_1industry(operator=cls._operator)
        cls._industry_handler = DcIndustryDailyHandler(
            operator=cls._operator
        ).obtain_data(para=cls._long_para)
        CalendarHandler(operator=cls._operator).obtain_data()

    def _setup_always(self) -> None:
        pass

    def test_pattern(self):
        """
        测试PatternAnalyst

        :return:
        """
        handler = PatternAnalyst(
            datasource_op=self._select_op, operator=self._insert_op
        )
        handler.calculate(span=self._long_para.span)
        select_para = (
            Para()
            .with_source(SourceType.ANA)
            .with_freq(FreqType.DAY)
            .with_fuquan(FuquanType.HFQ)
            .with_code("000001")
            .with_analysis(AnalystType.PATTERN)
        )
        data = handler.select_data(select_para)
        assert dataframe_is_valid(data)

    def test_stat(self):
        """
        测试StatAnalyst

        :return:
        """
        handler = StatZdfAnalyst(
            datasource_op=self._select_op, operator=self._insert_op
        )
        handler.calculate(span=self._long_para.span)
        select_para = (
            Para()
            .with_source(SourceType.ANA)
            .with_freq(FreqType.DAY)
            .with_fuquan(FuquanType.HFQ)
            .with_code("000001")
            .with_analysis(AnalystType.STAT_ZDF)
        )
        data = handler.select_data(select_para)
        assert dataframe_is_valid(data)
