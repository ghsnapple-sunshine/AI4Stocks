from buffett.adapter.pendulum import Date
from buffett.analysis import Para
from buffett.common.target import Target
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.stock import BsDailyHandler, DcDailyHandler
from buffett.download.maintain.daily_mtain import StockDailyMaintain
from test import Tester, create_ex_1stock, create_1stock


class TestStockDailyMaintain(Tester):
    _calendar_handler = None
    _bs_handler = None
    _dc_handler = None

    @classmethod
    def _setup_oncemore(cls):
        cls._calendar_handler = CalendarHandler(operator=cls._operator)
        cls._calendar_handler.obtain_data()
        cls._bs_handler = BsDailyHandler(operator=cls._operator)
        cls._dc_handler = DcDailyHandler(operator=cls._operator)
        cls._mtain = StockDailyMaintain(operator=cls._operator)
        StockDailyMaintain.set_save_report(save=False)

    def _setup_always(self) -> None:
        pass

    def test_000605(self):
        """
        现网异常数据
        （2000/1-2000/6缺失数据）

        :return:
        """
        create_ex_1stock(
            operator=self._operator, target=Target("000605"), source="both"
        )
        para = Para().with_start_n_end(Date(1999, 12, 1), Date(2000, 12, 1))
        self._bs_handler.obtain_data(para=para)
        self._dc_handler.obtain_data(para=para)
        assert not self._mtain.run()

    def test_000001(self):
        """
        测试正常运行

        :return:
        """
        create_1stock(operator=self._operator, source="both")
        para = Para().with_start_n_end(Date(2022, 1, 1), Date.today())
        self._bs_handler.obtain_data(para=para)
        self._dc_handler.obtain_data(para=para)
        assert self._mtain.run()
