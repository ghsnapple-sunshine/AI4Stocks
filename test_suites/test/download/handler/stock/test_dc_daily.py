from buffett.adapter.akshare import ak
from buffett.common.constants.col import DATE
from buffett.common.logger import Logger
from buffett.common.pendulum import Date
from buffett.common.target import Target
from buffett.common.tools import dataframe_not_valid
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

    def test_download_20years(self):
        """
        测试下载与数据库保存是否一致

        :return:
        """
        create_1stock(self._operator)
        tbl = self._hdl._download(para=self._great_para)
        self._hdl.obtain_data(para=self._great_para)
        db = self._hdl.select_data(para=self._great_para).reset_index()
        db[DATE] = db[DATE].apply(lambda x: str(x))
        assert self.dataframe_almost_equals(tbl, db, join_columns=[DATE])

    def test_official(self):
        """
        测试数据库与商用数据库进行对比

        :return:
        """
        create_1stock(self._operator)
        self._hdl.obtain_data(para=self._great_para)
        test_data = self._hdl.select_data(para=self._great_para).reset_index()
        test_data[DATE] = test_data[DATE].apply(lambda x: str(x))
        official_data = self.official_select(
            DcDailyHandler, para=self._great_para
        ).reset_index()
        if dataframe_not_valid(official_data):
            Logger.warning("数据库无此表格。")
        else:
            official_data[DATE] = official_data[DATE].apply(lambda x: str(x))
            assert self.compare_dataframe(test_data, official_data)

    """
    def test_000022(self):
        create_ex_1stock(self._operator, '000022')
        self._hdl.obtain_data(para=self._short_para)
        assert True
    """
