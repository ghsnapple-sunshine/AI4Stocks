from buffett.analysis import Para
from buffett.analysis.study.stat_zdf import StatZdfAnalyst
from buffett.common.pendulum import Date, DateSpan
from buffett.common.target import Target
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.stock import DcDailyHandler
from test import create_ex_1stock
from test.analysis.analysis_tester import AnalysisTester


class TestAnalystIrregular(AnalysisTester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_stat_001227(self):
        """
        001227在目标测试段没有数据

        :return:
        """
        # prepare

        create_ex_1stock(operator=self._operator, target=Target("001227"), is_sse=True)
        create_ex_1stock(operator=self._operator, target=Target("001227"), is_sse=False)
        select_para = Para().with_start_n_end(Date(2000, 1, 1), Date(2022, 12, 31))
        DcDailyHandler(operator=self._operator).obtain_data(para=select_para)
        CalendarHandler(operator=self._operator).obtain_data()
        # run
        handler = StatZdfAnalyst(
            datasource_op=self._select_op, operator=self._insert_op
        )
        calc_span = DateSpan(Date(2000, 1, 1), Date(2021, 12, 31))
        handler.calculate(span=calc_span)
