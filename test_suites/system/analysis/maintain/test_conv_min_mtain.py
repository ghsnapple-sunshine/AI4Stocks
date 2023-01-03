from unittest.mock import patch

from buffett.adapter.pandas import DataFrame
from buffett.analysis.maintain import ConvertStockMinuteAnalystMaintain
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.magic import get_name
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.mysql import Operator
from system.mock_tester import MockTester


class ConvertStockMinuteAnalystMaintainForMock(ConvertStockMinuteAnalystMaintain):
    def __init__(self, ana_rop: Operator, stk_op: Operator, ana_wop: Operator):
        super(ConvertStockMinuteAnalystMaintainForMock, self).__init__(
            ana_op=ana_rop, stk_rop=stk_op
        )
        self._ana_wop = ana_wop


class TestConvertStockMinuteAnalyst(MockTester):
    @classmethod
    def _setup_oncemore(cls):
        cls._mtain = ConvertStockMinuteAnalystMaintainForMock(
            ana_rop=cls._ana_op, ana_wop=cls._operator, stk_op=cls._stk_op
        )

    def _setup_always(self) -> None:
        pass

    def test_000021(self):
        stock_list = DataFrame({CODE: ["000021"], NAME: [""]})
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
                self._mtain.run()
                assert True  # 能运行即可

    def test_000023(self):
        stock_list = DataFrame({CODE: ["000023"], NAME: [""]})
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
                self._mtain.run()
                assert True  # 能运行即可
