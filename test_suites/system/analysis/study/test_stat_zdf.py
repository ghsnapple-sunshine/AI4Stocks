from unittest.mock import patch

from buffett.adapter.pandas import DataFrame
from buffett.analysis import Para
from buffett.analysis.recorder.analysis_recorder import AnalysisRecorder
from buffett.analysis.study import FuquanAnalystV2, StatZdfAnalyst
from buffett.analysis.study.tools import TableNameTool
from buffett.analysis.types import AnalystType
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.analysis import FQ_FAC_META, ANA_ZDF_META
from buffett.common.constants.table import FQ_FAC_V2
from buffett.common.magic import get_name
from buffett.common.tools import dataframe_is_valid
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.mysql import Operator
from buffett.download.types import SourceType, FreqType, FuquanType
from system.mock_tester import MockTester


class StatZdfAnalystForMock(StatZdfAnalyst):
    def __init__(self, ana_rop: Operator, stk_op: Operator, ana_wop: Operator):
        super(StatZdfAnalystForMock, self).__init__(ana_op=ana_rop, stk_op=stk_op)
        self._ana_wop = ana_wop


class TestFuquanAnalyst(MockTester):
    _analyst = None

    @classmethod
    def _setup_oncemore(cls):
        cls._analyst = StatZdfAnalystForMock(
            ana_rop=cls._ana_op, ana_wop=cls._operator, stk_op=cls._stk_op
        )

    def _setup_always(self) -> None:
        pass

    def test_000593(self):
        """
        股票无法计算得出fuquan factor

        :return:
        """
        stock_list = DataFrame({CODE: ["000593"], NAME: [""]})
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
                    AnalysisRecorder,
                    get_name(AnalysisRecorder.select_data),
                    return_value=None,
                ):
                    self._analyst.calculate(span=self._great_para.span)
        table_name = TableNameTool.get_by_code(
            para=Para()
            .with_code("000593")
            .with_source(SourceType.ANA)
            .with_freq(FreqType.MIN5)
            .with_fuquan(FuquanType.HFQ)
            .with_analysis(AnalystType.STAT_ZDF)
        )
        data = self._operator.select_data(name=table_name, meta=ANA_ZDF_META)
        assert dataframe_is_valid(data)
