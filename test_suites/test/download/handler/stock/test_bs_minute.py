from buffett.adapter.baostock import bs
from buffett.common.constants.col import DATETIME
from buffett.common.logger import Logger
from buffett.common.pendulum import Date, DateTime, convert_date
from buffett.common.target import Target
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.stock import BsMinuteHandler
from buffett.download.types import FuquanType
from test import Tester, create_2stocks, create_ex_1stock, DbSweeper, create_1stock


class TestBsStockMinuteHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = BsMinuteHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        DbSweeper.erase()

    def test_download_1_month(self) -> None:
        stocks = create_2stocks(self._operator, is_sse=False)
        tbls = self._hdl.obtain_data(para=self._short_para)
        assert stocks.shape[0] == len(tbls)

    def test_download_1_year(self) -> None:
        stocks = create_2stocks(self._operator, is_sse=False)
        tbls = self._hdl.obtain_data(para=self._long_para)
        assert stocks.shape[0] == len(tbls)

    def test_000795(self) -> None:
        stocks = create_ex_1stock(self._operator, Target("000795"), is_sse=False)
        para = Para().with_start_n_end(Date(2002, 1, 1), Date(2003, 1, 1))
        tbls = self._hdl.obtain_data(para=para)
        assert stocks.shape[0] == len(tbls)

    def test_download_in_a_day(self) -> None:
        create_1stock(self._operator, is_sse=False)
        # 正常下载数据
        para = Para().with_start_n_end(
            start=DateTime(year=2022, month=9, day=30, hour=9),
            end=DateTime(year=2022, month=9, day=30, hour=17),
        )
        self._hdl.obtain_data(para=para)
        para = Para().with_code("000001").with_fuquan(FuquanType.BFQ)
        data = self._hdl.select_data(para=para)
        assert convert_date(data.index.max()) == DateTime(
            year=2022, month=9, day=30, hour=15
        )

    def test_download_in_a_day2(self) -> None:
        create_1stock(self._operator, is_sse=False)
        # 下载收盘后的数据
        para = Para().with_start_n_end(
            start=DateTime(year=2022, month=9, day=30, hour=17),
            end=DateTime(year=2022, month=9, day=30, hour=18),
        )
        self._hdl.obtain_data(para=para)
        para = Para().with_code("000795").with_fuquan(FuquanType.BFQ)
        data = self._hdl.select_data(para=para)
        assert dataframe_not_valid(data)

    def test_download_in_a_day3(self) -> None:
        create_1stock(self._operator, is_sse=False)
        # 下载收盘后的数据
        try:
            Para().with_start_n_end(
                start=Date(year=2022, month=9, day=30),
                end=Date(year=2022, month=9, day=30),
            )
        except ValueError:
            assert True

    def test_301369(self) -> None:
        create_ex_1stock(self._operator, Target("301369"), is_sse=False)
        para = Para().with_start_n_end(Date(2000, 1, 1), Date.today())
        self._hdl.obtain_data(para=para)

    def test_repeat_download(self) -> None:
        """
        测试重复下载（现网场景）

        :return:
        """
        create_1stock(self._operator, is_sse=False)
        select_para = Para().with_code("000001").with_fuquan(FuquanType.BFQ)
        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 5), end=Date(2022, 1, 7))
        )
        db = self._hdl.select_data(para=select_para)
        assert db.shape[0] == 2 * 240 / 5  # 2022/1/5, 2022/1/6

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 5), end=Date(2022, 1, 8))
        )
        db = self._hdl.select_data(para=select_para)
        # 2022/1/5, 2022/1/6, 2022/1/7
        assert db.shape[0] == 3 * 240 / 5

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 8))
        )
        db = self._hdl.select_data(para=select_para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7
        assert db.shape[0] == 4 * 240 / 5

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 3), end=Date(2022, 1, 9))
        )
        db = self._hdl.select_data(para=select_para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/3为公休日, 2022/1/8为周六
        assert db.shape[0] == 4 * 240 / 5

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 3), end=Date(2022, 1, 11))
        )
        db = self._hdl.select_data(para=select_para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/10, 2022/1/3为公休日, 2022/1/8为周六
        assert db.shape[0] == 5 * 240 / 5

    def test_000003(self):
        """
        测试退市股票下载+测试下载数据量
        :return:
        """
        create_ex_1stock(self._operator, target=Target("000003"), is_sse=False)
        para = Para().with_start_n_end(Date(2000, 1, 15), Date(2000, 3, 28))
        self._hdl.obtain_data(para=para)
        data = self._hdl.select_data(
            para=Para().with_fuquan(FuquanType.BFQ).with_code("000003")
        )
        origin_data = bs.query_history_k_data_plus(
            code="sz.000003",
            fields="time,open,high,low,close,volume,amount",
            frequency="5",
            start_date=para.span.start.format("YYYY-MM-DD"),
            end_date=para.span.end.subtract(days=1).format("YYYY-MM-DD"),
            adjustflag=FuquanType.BFQ.bs_format(),
        )
        assert data.shape[0] == origin_data.shape[0] != 0

    def test_official(self):
        """
        测试数据库与商用数据库进行对比

        :return:
        """
        create_1stock(self._operator, is_sse=False)
        self._hdl.obtain_data(para=self._long_para)
        test_data = self._hdl.select_data(para=self._long_para).reset_index()
        test_data[DATETIME] = test_data[DATETIME].apply(lambda x: str(x))
        official_data = self.official_select(
            BsMinuteHandler, para=self._long_para
        ).reset_index()
        if dataframe_not_valid(official_data):
            Logger.warning("数据库无此表格。")
        else:
            official_data[DATETIME] = official_data[DATETIME].apply(lambda x: str(x))
            assert self.compare_dataframe(test_data, official_data)
