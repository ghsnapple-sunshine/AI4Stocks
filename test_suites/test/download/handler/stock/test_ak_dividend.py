from buffett.adapter.pendulum import Date
from buffett.common.pendulum import DateSpan
from buffett.download import Para
from buffett.download.handler.stock.ak_dividend import AkStockDividendHandler
from test import DbSweeper, Tester


class TestAkStockDividendHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = AkStockDividendHandler(operator=cls._operator)
        cls._short_span = DateSpan(start=Date(2020, 1, 1), end=Date(2021, 1, 1))
        cls._short_span2 = DateSpan(start=Date(2020, 10, 31), end=Date(2021, 1, 1))
        cls._long_span = DateSpan(start=Date(2020, 1, 1), end=Date(2022, 1, 1))

    def _setup_always(self) -> None:
        DbSweeper.erase()

    def test_download(self):
        para = Para().with_span(self._short_span)
        df1 = self._hdl.obtain_data(para=para)
        df2 = self._hdl.select_data(para=para)
        assert df1.shape == df2.shape

    def test_download_n_combine(self):
        # S1
        para1 = Para().with_span(self._long_span)
        df1 = self._hdl.obtain_data(para=para1)

        # S2
        DbSweeper.cleanup()
        para2 = Para().with_span(self._long_span)
        self._hdl.obtain_data(para=para1)
        self._hdl.obtain_data(para=para2)
        df2 = self._hdl.obtain_data(para=para2)
        assert df2 is None
        df3 = self._hdl.select_data(para=para2)
        assert df1.shape == df3.shape
