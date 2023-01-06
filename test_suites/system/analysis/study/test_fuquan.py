from unittest.mock import patch

from buffett.adapter.pandas import DataFrame
from buffett.analysis.study import FuquanAnalyst
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.analysis import FQ_FAC_META
from buffett.common.constants.table import FQ_FAC_V2
from buffett.common.magic import get_name
from buffett.common.tools import dataframe_is_valid
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from system.mock_tester import MockTester


class TestFuquanAnalyst(MockTester):
    _analyst = None

    @classmethod
    def _setup_oncemore(cls):
        cls._analyst = FuquanAnalyst(
            ana_rop=cls._ana_op, ana_wop=cls._operator, stk_rop=cls._stk_op
        )

    def _setup_always(self) -> None:
        pass

    def test_000980(self):
        """
        股票无法计算得出fuquan factor

        :return:
        """
        self._atom_test("000980")

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
                self._analyst.calculate(span=self._great_para.span)
        data = self._operator.select_data(name=FQ_FAC_V2, meta=FQ_FAC_META)
        assert dataframe_is_valid(data)
