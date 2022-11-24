from buffett.adapter.pendulum import Date
from buffett.common.constants.table import TRA_CAL
from buffett.common.pendulum import DateSpan
from buffett.download import Para
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.index.ak_daily import AkIndexDailyHandler
from test import Tester, DbSweeper, create_1index, create_2indexs


class TestAkIndexDailyHandler(Tester):
    _cal_hdl = None

    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = AkIndexDailyHandler(operator=cls._operator)
        cls._cal_hdl = CalendarHandler(operator=cls._operator)
        cls._cal_hdl.obtain_data()

    def _setup_always(self) -> None:
        DbSweeper.erase_except(name=TRA_CAL)

    def test_repeat_download(self) -> None:
        """
        测试重复下载（现网场景）

        :return:
        """
        create_1index(operator=self._operator)
        para = Para().with_code("000001")
        self._hdl.obtain_data(
            span=DateSpan(start=Date(2022, 1, 5), end=Date(2022, 1, 7))
        )
        db = self._hdl.select_data(para=para)
        assert db.shape[0] == 2  # 2022/1/5, 2022/1/6

        self._hdl.obtain_data(
            span=DateSpan(start=Date(2022, 1, 5), end=Date(2022, 1, 8))
        )
        db = self._hdl.select_data(para=para)
        # 2022/1/5, 2022/1/6, 2022/1/7
        assert db.shape[0] == 3

        self._hdl.obtain_data(
            span=DateSpan(start=Date(2022, 1, 4), end=Date(2022, 1, 8))
        )
        db = self._hdl.select_data(para=para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7
        assert db.shape[0] == 4

        self._hdl.obtain_data(
            span=DateSpan(start=Date(2022, 1, 3), end=Date(2022, 1, 9))
        )
        db = self._hdl.select_data(para=para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/3为公休日, 2022/1/8为周六
        assert db.shape[0] == 4

        self._hdl.obtain_data(
            span=DateSpan(start=Date(2022, 1, 3), end=Date(2022, 1, 11))
        )
        db = self._hdl.select_data(para=para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/10, 2022/1/3为公休日, 2022/1/8为周六
        assert db.shape[0] == 5

    def test_download_1month(self) -> None:
        create_2indexs(self._operator)
        self._hdl.obtain_data(span=self._short_para.span)
        df1 = self._hdl.select_data(para=Para().with_code("000001"))
        df2 = self._hdl.select_data(para=Para().with_code("399006"))
        df3 = self._cal_hdl.select_data(para=self._short_para)
        assert df1.shape[0] == df2.shape[0] == df3.shape[0]

    def test_download_1year(self) -> None:
        create_2indexs(self._operator)
        self._hdl.obtain_data(span=self._long_para.span)
        df1 = self._hdl.select_data(para=Para().with_code("000001"))
        df2 = self._hdl.select_data(para=Para().with_code("399006"))
        df3 = self._cal_hdl.select_data(para=self._long_para)
        assert df1.shape[0] == df2.shape[0] == df3.shape[0]
