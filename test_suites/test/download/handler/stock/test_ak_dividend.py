from buffett.adapter.pendulum import Date
from buffett.download import Para
from buffett.download.handler.stock.ak_dividend import AkStockDividendHandler
from test import SingletonTester, DbSweeper


class TestAkStockDividendHandler(SingletonTester):
    def _more_init(self):
        self._hdl = AkStockDividendHandler(operator=self._operator)

    def setUp(self) -> None:
        DbSweeper.cleanup()

    def test_download(self):
        para = Para().with_start_n_end(start=Date(2020, 1, 1), end=Date(2021, 1, 1))
        df1 = self._hdl.obtain_data(para=para)
        df2 = self._hdl.select_data(para=para)
        assert df1.shape == df2.shape

    def test_download_n_combine(self):
        # S1
        para1 = Para().with_start_n_end(start=Date(2020, 1, 1), end=Date(2022, 1, 1))
        df1 = self._hdl.obtain_data(para=para1)

        # S2
        DbSweeper.cleanup()
        para1 = Para().with_start_n_end(start=Date(2020, 1, 1), end=Date(2021, 1, 30))
        para2 = Para().with_start_n_end(start=Date(2020, 1, 10), end=Date(2022, 1, 1))
        self._hdl.obtain_data(para=para1)
        self._hdl.obtain_data(para=para2)
        df2 = self._hdl.obtain_data(para=para2)
        assert df2 is None
        df3 = self._hdl.select_data(para=para2)
        assert df1.shape == df3.shape
