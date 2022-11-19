from buffett.common.pendelum import Date
from buffett.common.stock import Code
from buffett.download import Para
from buffett.download.handler.stock.ak_daily import AkDailyHandler
from buffett.download.types import FuquanType
from test import Tester, create_1stock, create_2stocks, DbSweeper


class AkDailyHandlerTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = AkDailyHandler(cls._operator)

    def _setup_always(self) -> None:
        DbSweeper.erase()

    def test_repeat_download(self) -> None:
        """
        测试重复下载（现网场景）

        :return:
        """
        create_1stock(self._operator)
        select_para = Para().with_code(Code("000001")).with_fuquan(FuquanType.BFQ)
        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 5), end=Date(2022, 1, 7))
        )
        db = self._hdl.select_data(para=select_para)
        assert db.shape[0] == 2  # 2022/1/5, 2022/1/6

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 5), end=Date(2022, 1, 8))
        )
        db = self._hdl.select_data(para=select_para)
        # 2022/1/5, 2022/1/6, 2022/1/7
        assert db.shape[0] == 3

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 8))
        )
        db = self._hdl.select_data(para=select_para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7
        assert db.shape[0] == 4

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 3), end=Date(2022, 1, 9))
        )
        db = self._hdl.select_data(para=select_para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/3为公休日, 2022/1/8为周六
        assert db.shape[0] == 4

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 3), end=Date(2022, 1, 11))
        )
        db = self._hdl.select_data(para=select_para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/10, 2022/1/3为公休日, 2022/1/8为周六
        assert db.shape[0] == 5

    def test_download_1_month(self) -> None:
        stocks = create_2stocks(self._operator)
        tbls = self._hdl.obtain_data(para=self._short_para)
        assert stocks.shape[0] * 3 == len(tbls)

    def test_download_10_years(self) -> None:
        stocks = create_2stocks(self._operator)
        tbls = self._hdl.obtain_data(para=self._long_para)
        assert stocks.shape[0] * 3 == len(tbls)
