from buffett.adapter.akshare import ak
from buffett.common.pendulum import Date
from buffett.download import Para
from buffett.download.handler.stock.dc_daily import DcDailyHandler
from buffett.download.types import FuquanType
from test import Tester, create_1stock, create_2stocks, DbSweeper, create_ex_1stock


class TestDcDailyHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = DcDailyHandler(cls._operator)

    def _setup_always(self):
        DbSweeper.erase()

    def test_repeat_download(self):
        """
        测试重复下载（现网场景）

        :return:
        """
        create_1stock(self._operator)
        select_para = Para().with_code("000001").with_fuquan(FuquanType.BFQ)
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

    def test_download_1month(self):
        stocks = create_2stocks(self._operator)
        tbls = self._hdl.obtain_data(para=self._short_para)
        assert stocks.shape[0] * 3 == len(tbls)

    def test_download_1year(self):
        stocks = create_2stocks(self._operator)
        tbls = self._hdl.obtain_data(para=self._long_para)
        assert stocks.shape[0] * 3 == len(tbls)

    def test_download_tuishi(self):
        """
        测试退市股票下载+测试下载数据量
        :return:
        """
        create_ex_1stock(self._operator, code="000003")
        para = Para().with_start_n_end(Date(2000, 1, 15), Date(2000, 3, 28))
        self._hdl.obtain_data(para=para)
        data = self._hdl.select_data(
            para=Para().with_fuquan(FuquanType.BFQ).with_code("000003")
        )
        origin_data = ak.stock_zh_a_hist(
            symbol="000003",
            period="daily",
            start_date=para.span.start.format("YYYYMMDD"),
            end_date=para.span.end.subtract(days=1).format("YYYYMMDD"),
            adjust=FuquanType.BFQ.ak_format(),
        )
        assert data.shape[0] == origin_data.shape[0] != 0
