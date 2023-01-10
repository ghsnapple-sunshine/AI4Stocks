from buffett.analysis import Para
from buffett.analysis.study.stat_zdf import StatZdfAnalyst
from buffett.common.pendulum import Date, DateSpan
from buffett.common.target import Target
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.concept import DcConceptDailyHandler
from buffett.download.handler.index import DcIndexDailyHandler
from buffett.download.handler.industry import DcIndustryDailyHandler
from buffett.download.handler.stock import DcDailyHandler
from test import (
    create_ex_1stock,
    create_1stock,
    create_1index,
    create_1concept,
    create_1industry,
)
from test.analysis.analysis_tester import AnalysisTester


class TestAnalystIrregular(AnalysisTester):
    @classmethod
    def _setup_oncemore(cls):
        create_1stock(operator=cls._stk_rop, source="both")
        DcDailyHandler(operator=cls._stk_rop).obtain_data(para=cls._long_para)
        create_1index(operator=cls._stk_rop)
        DcIndexDailyHandler(operator=cls._stk_rop).obtain_data(para=cls._long_para)
        create_1concept(operator=cls._stk_rop)
        DcConceptDailyHandler(operator=cls._stk_rop).obtain_data(para=cls._long_para)
        create_1industry(operator=cls._stk_rop)
        DcIndustryDailyHandler(operator=cls._stk_rop).obtain_data(para=cls._long_para)
        CalendarHandler(operator=cls._stk_rop).obtain_data()

    def _setup_always(self) -> None:
        pass

    def test_stat_001227(self):
        """
        001227在目标测试段没有数据

        :return:
        """
        # prepare
        create_ex_1stock(
            operator=self._operator, target=Target("001227"), source="both"
        )
        select_para = Para().with_start_n_end(Date(2000, 1, 1), Date(2022, 12, 31))
        DcDailyHandler(operator=self._operator).obtain_data(para=select_para)
        CalendarHandler(operator=self._operator).obtain_data()
        # run
        handler = StatZdfAnalyst(
            stk_rop=self._stk_rop, ana_rop=self._ana_rop, ana_wop=self._ana_wop
        )
        calc_span = DateSpan(Date(2000, 1, 1), Date(2021, 12, 31))
        handler.calculate(span=calc_span)
