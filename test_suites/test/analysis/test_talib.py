from buffett.adapter.talib import PatternRecognize
from buffett.download.handler.stock import DcDailyHandler
from buffett.download.types import FuquanType
from test import Tester, create_1stock


class TestTalib(Tester):
    _handler = None

    @classmethod
    def _setup_oncemore(cls):
        create_1stock(operator=cls._operator)
        cls._handler = DcDailyHandler(operator=cls._operator)
        cls._handler.obtain_data(para=cls._great_para)
        cls._data = cls._handler.select_data(
            para=cls._great_para.clone().with_fuquan(FuquanType.HFQ)
        )

    def _setup_always(self) -> None:
        pass

    def test_two_crows(self):
        result = PatternRecognize.two_crows(self._data)
        assert self._data.shape[0] == result.shape[0]

    def test_all(self):
        result = PatternRecognize.all(self._data)
        assert result.shape == (self._data.shape[0], 61)
