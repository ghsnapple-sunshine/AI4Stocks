from unittest.mock import patch

from buffett.adapter.pandas import DataFrame
from buffett.analysis.maintain.min_bfq import StockMinuteBfqMaintain
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.magic import get_name
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from system.mock_tester import MockTester


class TestConvertStockMinuteAnalyst(MockTester):
    @classmethod
    def _setup_oncemore(cls):
        cls._mtain = StockMinuteBfqMaintain(
            mtain_wop=cls._operator, ana_rop=cls._ana_op, stk_rop=cls._stk_op
        )

    def _setup_always(self) -> None:
        pass

    def test_000001(self):
        stock_list = DataFrame({CODE: ["000001"], NAME: [""]})
        with patch.object(
            SseStockListHandler,
            get_name(SseStockListHandler.select_data),
            return_value=stock_list,
        ), patch.object(
            BsStockListHandler,
            get_name(BsStockListHandler.select_data),
            return_value=stock_list,
        ):
            self._mtain.run()
            assert True  # 能运行即可
