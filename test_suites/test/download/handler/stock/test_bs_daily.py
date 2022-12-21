from buffett.adapter.baostock import bs
from buffett.common.constants.col import DATE, ZF, ZDE
from buffett.common.pendulum import Date
from buffett.common.target import Target
from buffett.download import Para
from buffett.download.handler.stock.bs_daily import BsDailyHandler
from buffett.download.types import FuquanType
from test import Tester, create_1stock, create_2stocks, DbSweeper, create_ex_1stock


class TestBsDailyHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = BsDailyHandler(cls._operator)

    def _setup_always(self):
        DbSweeper.erase()

    def test_repeat_download(self):
        """
        测试重复下载（现网场景）

        :return:
        """
        create_1stock(self._operator, source="bs")
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
        stocks = create_2stocks(self._operator, source="bs")
        tbls = self._hdl.obtain_data(para=self._short_para)
        assert stocks.shape[0] * 2 == len(tbls)

    def test_download_1year(self):
        stocks = create_2stocks(self._operator, source="bs")
        tbls = self._hdl.obtain_data(para=self._long_para)
        assert stocks.shape[0] * 2 == len(tbls)

    def test_000003(self):
        """
        测试退市股票下载+测试下载数据量
        :return:
        """
        create_ex_1stock(self._operator, target=Target("000003"), source="bs")
        para = Para().with_start_n_end(Date(2000, 1, 15), Date(2000, 3, 28))
        self._hdl.obtain_data(para=para)
        data = self._hdl.select_data(
            para=Para().with_fuquan(FuquanType.BFQ).with_code("000003")
        )
        origin_data = bs.query_history_k_data_plus(
            code="sz.000003",
            fields="date,open,high,low,close,preclose,volume,amount,turn,pctChg",
            frequency="d",
            start_date=para.span.start.format("YYYY-MM-DD"),
            end_date=para.span.end.subtract(days=1).format("YYYY-MM-DD"),
            adjustflag="3",
        )
        assert data.shape[0] == origin_data.shape[0] != 0

    def test_download_20years(self):
        """
        测试下载与数据库保存是否一致

        :return:
        """
        create_1stock(self._operator, source="bs")
        tbl = self._hdl._download(para=self._great_para)
        self._hdl.obtain_data(para=self._great_para)
        db = self._hdl.select_data(para=self._great_para).reset_index()
        db[DATE] = db[DATE].apply(lambda x: str(x))
        del db[ZF], db[ZDE]
        assert self.dataframe_almost_equals(tbl, db, join_columns=[DATE])

    def test_000022(self):
        """
        测试退市股票能否正常下载

        :return:
        """
        create_ex_1stock(self._operator, Target("000022"), source="bs")
        self._hdl.obtain_data(para=self._great_para)
        assert True

    def test_000001(self):
        """
        测试成交量为0的时间段

        :return:
        """
        create_1stock(self._operator, source="bs")
        self._hdl.obtain_data(
            para=Para().with_start_n_end(Date(1992, 2, 1), Date(1992, 3, 1))
        )
        assert True
