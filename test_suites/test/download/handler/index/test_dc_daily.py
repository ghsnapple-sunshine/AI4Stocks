from buffett.adapter.pendulum import Date
from buffett.common.constants.table import TRA_CAL
from buffett.common.target import Target
from buffett.download import Para
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.index import DcIndexDailyHandler
from test import Tester, DbSweeper, create_1index, create_2indexs, create_ex_1index


class TestDcIndexDailyHandler(Tester):
    _cal_hdl = None

    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = DcIndexDailyHandler(operator=cls._operator)
        cls._cal_hdl = CalendarHandler(operator=cls._operator)
        cls._cal_hdl.obtain_data()

    def _setup_always(self) -> None:
        DbSweeper.erase_except(excepts=TRA_CAL)

    def test_repeat_download(self) -> None:
        """
        测试重复下载（现网场景）

        :return:
        """
        create_1index(operator=self._operator)
        para = Para().with_code("000001")
        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 5), end=Date(2022, 1, 7))
        )
        db = self._hdl.select_data(para=para)
        assert db.shape[0] == 2  # 2022/1/5, 2022/1/6

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 5), end=Date(2022, 1, 8))
        )
        db = self._hdl.select_data(para=para)
        # 2022/1/5, 2022/1/6, 2022/1/7
        assert db.shape[0] == 3

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 4), end=Date(2022, 1, 8))
        )
        db = self._hdl.select_data(para=para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7
        assert db.shape[0] == 4

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 3), end=Date(2022, 1, 9))
        )
        db = self._hdl.select_data(para=para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/3为公休日, 2022/1/8为周六
        assert db.shape[0] == 4

        self._hdl.obtain_data(
            para=Para().with_start_n_end(start=Date(2022, 1, 3), end=Date(2022, 1, 11))
        )
        db = self._hdl.select_data(para=para)
        # 2022/1/4, 2022/1/5, 2022/1/6，2022/1/7, 2022/1/10, 2022/1/3为公休日, 2022/1/8为周六
        assert db.shape[0] == 5

    def test_download_1month(self) -> None:
        create_2indexs(self._operator)
        self._hdl.obtain_data(para=self._short_para)
        df1 = self._hdl.select_data(para=Para().with_code("000001"))
        df2 = self._hdl.select_data(para=Para().with_code("399006"))
        df3 = self._cal_hdl.select_data(para=self._short_para)
        assert df1.shape[0] == df2.shape[0] == df3.shape[0]

    def test_download_1year(self) -> None:
        create_2indexs(self._operator)
        self._hdl.obtain_data(para=self._long_para)
        df1 = self._hdl.select_data(para=Para().with_code("000001"))
        df2 = self._hdl.select_data(para=Para().with_code("399006"))
        df3 = self._cal_hdl.select_data(para=self._long_para)
        assert df1.shape[0] == df2.shape[0] == df3.shape[0]

    def test_000188(self):
        """
        测试下载中国波指：无法下载到数据
        预期：不抛异常

        :return:
        """
        create_ex_1index(self._operator, Target("000188"))
        self._hdl.obtain_data(para=self._great_para)
