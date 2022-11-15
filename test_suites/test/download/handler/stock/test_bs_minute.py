from buffett.common.pendelum import Date, DateTime, convert_date
from buffett.common.stock import Code
from buffett.download import Para
from buffett.download.handler.stock.bs_minute import BsMinuteHandler
from buffett.download.types import FuquanType
from test import Tester, DbSweeper, create_2stocks, create_ex_1stock


class BsStockMinuteHandlerTest(Tester):
    def test_download_1_month(self) -> None:
        DbSweeper.cleanup()
        stocks = create_2stocks(self.operator)
        para = Para().with_start_n_end(Date(2022, 8, 1), Date(2022, 8, 31))
        tbls = BsMinuteHandler(operator=self.operator).obtain_data(para=para)
        assert stocks.shape[0] == len(tbls)

    def test_download_1_year(self) -> None:
        DbSweeper.cleanup()
        stocks = create_2stocks(self.operator)
        para = Para().with_start_n_end(Date(2021, 1, 1), Date(2021, 12, 31))
        tbls = BsMinuteHandler(operator=self.operator).obtain_data(para)
        assert stocks.shape[0] == len(tbls)

    def test_download_irregular(self) -> None:
        DbSweeper.cleanup()
        stocks = create_ex_1stock(self.operator, Code('000795'))
        para = Para().with_start_n_end(Date(2002, 1, 1), Date(2003, 1, 1))
        tbls = BsMinuteHandler(operator=self.operator).obtain_data(para=para)
        assert stocks.shape[0] == len(tbls)

    def test_download_in_a_day(self) -> None:
        DbSweeper.cleanup()
        create_ex_1stock(self.operator, Code('000795'))
        hdl = BsMinuteHandler(operator=self.operator)
        # 正常下载数据
        para = Para().with_start_n_end(start=DateTime(year=2022, month=9, day=30, hour=9),
                                       end=DateTime(year=2022, month=9, day=30, hour=17))
        hdl.obtain_data(para=para)
        para = Para().with_code(Code('000795')).with_fuquan(FuquanType.BFQ)
        data = hdl.select_data(para=para)
        assert convert_date(data.index.max()) == DateTime(year=2022, month=9, day=30, hour=15)

    def test_download_in_a_day2(self) -> None:
        DbSweeper.cleanup()
        create_ex_1stock(self.operator, Code('000795'))
        hdl = BsMinuteHandler(operator=self.operator)
        # 下载收盘后的数据
        para = Para().with_start_n_end(start=DateTime(year=2022, month=9, day=30, hour=17),
                                       end=DateTime(year=2022, month=9, day=30, hour=18))
        hdl.obtain_data(para=para)
        para = Para().with_code(Code('000795')).with_fuquan(FuquanType.BFQ)
        data = hdl.select_data(para=para)
        assert data.empty

    def test_download_in_a_day3(self) -> None:
        DbSweeper.cleanup()
        create_ex_1stock(self.operator, Code('000795'))
        # 下载收盘后的数据
        try:
            para = Para().with_start_n_end(start=Date(year=2022, month=9, day=30),
                                           end=Date(year=2022, month=9, day=30))
        except ValueError as e:
            assert True

    def test_download_20_years(self) -> None:
        DbSweeper.cleanup()
        create_ex_1stock(self.operator, Code('301369'))
        hdl = BsMinuteHandler(operator=self.operator)
        para = Para().with_start_n_end(Date(2000, 1, 1), Date.today())
        hdl.obtain_data(para=para)
