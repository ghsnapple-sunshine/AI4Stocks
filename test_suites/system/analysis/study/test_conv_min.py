from unittest.mock import patch

from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.analysis.recorder import AnalysisRecorder
from buffett.analysis.study import ConvertStockMinuteHfqAnalyst
from buffett.analysis.study.tools import TableNameTool
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import OPEN, CLOSE, HIGH, LOW, CJL
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.magic import get_name
from buffett.common.pendulum import DateSpan, Date
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.types import FreqType, SourceType, FuquanType
from system.mock_tester import MockTester


class TestConvertStockMinuteAnalyst(MockTester):
    @classmethod
    def _setup_oncemore(cls):
        cls._analyst = ConvertStockMinuteHfqAnalyst(
            ana_rop=cls._ana_op, ana_wop=cls._operator, stk_rop=cls._stk_op
        )

    def _setup_always(self) -> None:
        pass

    def test_000001(self):
        """
        测试现网报错数据
        市场中有NA数据

        :return:
        """
        self._atom_test("000001")

    def test_600004(self):
        """
        测试现网报错数据
        开盘前(9:30前)有数据

        :return:
        """
        self._atom_test("600004")

    def test_002350(self):
        """
        测试现网报错数据
        上市前(2010/2/3)有数据

        :return:
        """
        self._atom_test("002350")

    def _atom_test(self, code: str):
        stock_list = DataFrame({CODE: [code], NAME: [""]})
        with patch.object(
            SseStockListHandler,
            get_name(SseStockListHandler.select_data),
            return_value=stock_list,
        ), patch.object(
            BsStockListHandler,
            get_name(BsStockListHandler.select_data),
            return_value=stock_list,
        ), patch.object(
            AnalysisRecorder,
            get_name(AnalysisRecorder.select_data),
            return_value=None,
        ):
            self._analyst.calculate(
                span=DateSpan(start=Date(1990, 1, 1), end=Date(2022, 12, 1))
            )
        table_name = TableNameTool.get_by_code(
            para=Para()
            .with_code(code)
            .with_source(SourceType.ANA)
            .with_freq(FreqType.MIN5)
            .with_fuquan(FuquanType.HFQ)
            .with_analysis(AnalystType.CONV)
        )
        data = self._operator.select_data(name=table_name, meta=None)
        assert all(
            [
                pd.notna(x)
                for x in data[[OPEN, CLOSE, HIGH, LOW, CJL]].values.reshape(
                    (data.shape[0] * 5,)
                )
            ]
        )
