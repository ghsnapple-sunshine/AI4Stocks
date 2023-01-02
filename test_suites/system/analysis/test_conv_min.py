from unittest.mock import patch

from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.analysis.recorder.analysis_recorder import AnalysisRecorder
from buffett.analysis.study import ConvertStockMinuteAnalyst
from buffett.analysis.study.tools import TableNameTool
from buffett.analysis.types import AnalystType
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.magic import get_name
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.mysql import Operator
from buffett.download.types import FreqType, SourceType, FuquanType
from system.mock_tester import MockTester


class ConvertStockMinuteAnalystForMock(ConvertStockMinuteAnalyst):
    def __init__(self, ana_rop: Operator, stk_op: Operator, ana_wop: Operator):
        super(ConvertStockMinuteAnalystForMock, self).__init__(
            ana_op=ana_rop, stk_op=stk_op
        )
        self._ana_wop = ana_wop


class TestConvertStockMinuteAnalyst(MockTester):
    @classmethod
    def _setup_oncemore(cls):
        cls._analyst = ConvertStockMinuteAnalystForMock(
            ana_rop=cls._ana_op, ana_wop=cls._operator, stk_op=cls._stk_op
        )

    def _setup_always(self) -> None:
        pass

    def test_000001(self):
        stock_list = DataFrame({CODE: ["000001"], NAME: [""]})
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
                    self._analyst.calculate(span=self._short_para.span)
        table_name = TableNameTool.get_by_code(
            para=Para.from_base(self._short_para)
            .with_source(SourceType.ANA)
            .with_freq(FreqType.MIN5)
            .with_fuquan(FuquanType.HFQ)
            .with_analysis(AnalystType.CONV)
        )
        data = self._operator.select_data(name=table_name, meta=None)
        assert all(
            [pd.notna(x) for x in data.values.reshape((data.shape[0] * data.shape[1],))]
        )
