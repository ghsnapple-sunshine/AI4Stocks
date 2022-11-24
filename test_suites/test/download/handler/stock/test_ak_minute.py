from buffett.download.handler.stock.ak_minute import AkMinuteHandler
from test import Tester, create_2stocks


class TestAkStockMinuteHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = AkMinuteHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self) -> None:
        stocks = create_2stocks(self._operator)
        tbls = self._hdl.obtain_data(para=self._short_para)
        assert stocks.shape[0] == len(tbls)
