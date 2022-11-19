from buffett.common.pendulum import Date, DateTime, convert_date
from buffett.common.stock import Code
from buffett.download import Para
from buffett.download.handler.stock.bs_minute import BsMinuteHandler
from buffett.download.types import FuquanType
from test import Tester, create_2stocks, create_ex_1stock, DbSweeper


class BsStockMinuteHandlerTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = BsMinuteHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        pass
        DbSweeper.erase()

    def test_download_1_month(self) -> None:
        stocks = create_2stocks(self._operator)
        tbls = self._hdl.obtain_data(para=self._short_para)
        assert stocks.shape[0] == len(tbls)

    def test_download_1_year(self) -> None:
        stocks = create_2stocks(self._operator)
        tbls = self._hdl.obtain_data(para=self._long_para)
        assert stocks.shape[0] == len(tbls)

    def test_download_irregular(self) -> None:
        stocks = create_ex_1stock(self._operator, Code("000795"))
        para = Para().with_start_n_end(Date(2002, 1, 1), Date(2003, 1, 1))
        tbls = self._hdl.obtain_data(para=para)
        assert stocks.shape[0] == len(tbls)

    def test_download_in_a_day(self) -> None:
        create_ex_1stock(self._operator, Code("000795"))
        # 正常下载数据
        para = Para().with_start_n_end(
            start=DateTime(year=2022, month=9, day=30, hour=9),
            end=DateTime(year=2022, month=9, day=30, hour=17),
        )
        self._hdl.obtain_data(para=para)
        para = Para().with_code(Code("000795")).with_fuquan(FuquanType.BFQ)
        data = self._hdl.select_data(para=para)
        assert convert_date(data.index.max()) == DateTime(
            year=2022, month=9, day=30, hour=15
        )

    def test_download_in_a_day2(self) -> None:
        create_ex_1stock(self._operator, Code("000795"))
        # 下载收盘后的数据
        para = Para().with_start_n_end(
            start=DateTime(year=2022, month=9, day=30, hour=17),
            end=DateTime(year=2022, month=9, day=30, hour=18),
        )
        self._hdl.obtain_data(para=para)
        para = Para().with_code(Code("000795")).with_fuquan(FuquanType.BFQ)
        data = self._hdl.select_data(para=para)
        assert data.empty

    def test_download_in_a_day3(self) -> None:
        create_ex_1stock(self._operator, Code("000795"))
        # 下载收盘后的数据
        try:
            Para().with_start_n_end(
                start=Date(year=2022, month=9, day=30),
                end=Date(year=2022, month=9, day=30),
            )
        except ValueError:
            assert True

    def test_download_20_years(self) -> None:
        create_ex_1stock(self._operator, Code("301369"))
        para = Para().with_start_n_end(Date(2000, 1, 1), Date.today())
        self._hdl.obtain_data(para=para)
