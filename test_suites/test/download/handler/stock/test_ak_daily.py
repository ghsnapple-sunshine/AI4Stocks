from buffett.common.pendelum import Date, Duration
from buffett.common.stock import Code
from buffett.download import Para
from buffett.download.handler.stock.ak_daily import AkDailyHandler
from buffett.download.types import FuquanType
from test import Tester
from test.tools import create_1stock, create_2stocks


class AkDailyHandlerTest(Tester):
    def test_repeat_download(self) -> None:
        """
        测试重复下载（现网场景）

        :return:
        """
        stocks = create_1stock(self.operator)
        hdl = AkDailyHandler(self.operator)
        hdl.obtain_data(para=Para()
                        .with_start_n_end(start=Date(2022, 1, 5), end=Date(2022, 1, 7)))
        db = hdl.select_data(para=Para()
                             .with_code(Code('000001'))
                             .with_fuquan(FuquanType.BFQ))
        assert db.shape[0] == 2  # 2022/1/5, 2022/1/6

        hdl.obtain_data(para=Para()
                        .with_start_n_end(start=Date(2022, 1, 5), end=Date(2022, 1, 8)))
        db = hdl.select_data(para=Para()
                             .with_code(Code('000001'))
                             .with_fuquan(FuquanType.BFQ))
        assert db.shape[0] == 3  # 2022/1/5, 2022/1/6, 2022/1/7

        hdl.obtain_data(para=Para()
                        .with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 8)))
        db = hdl.select_data(para=Para()
                             .with_code(Code('000001'))
                             .with_fuquan(FuquanType.BFQ))
        assert db.shape[0] == 4  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7

        hdl.obtain_data(para=Para()
                        .with_start_n_end(start=Date(2022, 1, 3), end=Date(2022, 1, 9)))
        db = hdl.select_data(para=Para()
                             .with_code(Code('000001')).with_fuquan(FuquanType.BFQ))
        assert db.shape[0] == 4  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/3为公休日, 2022/1/8为周六

        hdl.obtain_data(para=Para()
                        .with_start_n_end(start=Date(2022, 1, 3), end=Date(2022, 1, 11)))
        db = hdl.select_data(para=Para()
                             .with_code(Code('000001'))
                             .with_fuquan(FuquanType.BFQ))
        assert db.shape[0] == 5  # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/10, 2022/1/3为公休日, 2022/1/8为周六

    def test_download_1_month(self) -> None:
        stocks = create_2stocks(self.operator)
        hdl = AkDailyHandler(self.operator)
        end_date = Date.today() - Duration(days=1)
        start_date = Date.today() - Duration(months=1)
        tbls = hdl.obtain_data(para=Para()
                               .with_start_n_end(start=start_date, end=end_date))
        assert stocks.shape[0] * 3 == len(tbls)

    def test_download_20_years(self) -> None:
        stocks = create_2stocks(self.operator)
        hdl = AkDailyHandler(self.operator)
        end_date = Date.today() - Duration(days=1)
        start_date = Date.today() - Duration(years=20)
        tbls = hdl.obtain_data(para=Para()
                               .with_start_n_end(start=start_date, end=end_date))
        assert stocks.shape[0] * 3 == len(tbls)
