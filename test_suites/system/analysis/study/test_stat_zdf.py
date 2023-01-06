from unittest.mock import patch

from buffett.adapter.pandas import DataFrame
from buffett.analysis import Para
from buffett.analysis.recorder import AnalysisRecorder
from buffett.analysis.study import StatZdfAnalyst
from buffett.analysis.study.tools import TableNameTool
from buffett.analysis.types import AnalystType
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.analysis import ANA_ZDF_META
from buffett.common.magic import get_name
from buffett.common.tools import dataframe_is_valid
from buffett.download.handler.concept import DcConceptListHandler
from buffett.download.handler.index import DcIndexListHandler
from buffett.download.handler.industry import DcIndustryListHandler
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.types import SourceType, FreqType, FuquanType
from system.mock_tester import MockTester


class TestFuquanAnalyst(MockTester):
    _analyst = None

    @classmethod
    def _setup_oncemore(cls):
        cls._analyst = StatZdfAnalyst(
            ana_rop=cls._ana_op, ana_wop=cls._operator, stk_rop=cls._stk_op
        )

    def _setup_always(self) -> None:
        pass

    def test_000593(self):
        """
        股票无法计算得出fuquan factor

        :return:
        """
        self._atom_test("000593")

    def test_600098(self):
        """
        计算涨跌幅得到inf
        （股票数据中有0数据）

        :return:
        """
        self._atom_test("600098")

    def test_002350(self):
        """
        计算涨跌幅中卡死
        （？）

        :return:
        """
        self._atom_test("002350")

    def _atom_test(self, code: str):
        stock_list = DataFrame({CODE: [code], NAME: [""]})
        with patch.object(
            SseStockListHandler,
            get_name(SseStockListHandler.select_data),
            return_value=stock_list,
        ):
            with patch.object(
                BsStockListHandler,
                get_name(BsStockListHandler.select_data),
                return_value=stock_list,
            ):
                with patch.object(
                    DcIndexListHandler,
                    get_name(DcIndexListHandler.select_data),
                    return_value=None,
                ):
                    with patch.object(
                        DcConceptListHandler,
                        get_name(DcConceptListHandler.select_data),
                        return_value=None,
                    ):
                        with patch.object(
                            DcIndustryListHandler,
                            get_name(DcIndustryListHandler.select_data),
                            return_value=None,
                        ):
                            with patch.object(
                                AnalysisRecorder,
                                get_name(AnalysisRecorder.select_data),
                                return_value=None,
                            ):
                                self._analyst.calculate(span=self._great_para.span)
        table_name = TableNameTool.get_by_code(
            para=Para()
            .with_code(code)
            .with_source(SourceType.ANA)
            .with_freq(FreqType.MIN5)
            .with_fuquan(FuquanType.HFQ)
            .with_analysis(AnalystType.STAT_ZDF)
        )
        data = self._operator.select_data(name=table_name, meta=ANA_ZDF_META)
        assert dataframe_is_valid(data)
