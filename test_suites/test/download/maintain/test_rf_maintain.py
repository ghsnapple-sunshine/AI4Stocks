from buffett.common.pendulum import Date
from buffett.common.target import Target
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.download import Para
from buffett.download.handler.reform import ReformHandler
from buffett.download.handler.stock import DcDailyHandler
from buffett.download.maintain import ReformMaintain
from test import Tester, create_2stocks, create_ex_1stock, DbSweeper


class TestReformMaintain(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._ak_handler = DcDailyHandler(operator=cls._operator)
        cls._rf_handler = ReformHandler(operator=cls._operator)
        cls._rf_mtain = ReformMaintain(operator=cls._operator)

    def _setup_always(self) -> None:
        DbSweeper.erase()

    def test_maintain(self):
        create_2stocks(operator=self._operator)
        self._ak_handler.obtain_data(para=self._short_para)
        self._rf_handler.reform_data()
        assert dataframe_not_valid(self._rf_mtain.run(save=False))

    def test_datanum_irregular(self):
        create_2stocks(operator=self._operator)
        self._ak_handler.obtain_data(para=self._long_para)
        self._rf_handler.reform_data()
        self._operator.execute(
            "delete from `dc_stock_dayinfo_2020_09_` where `date` > '2020-9-15'"
        )
        assert dataframe_is_valid(self._rf_mtain.run(save=False))

    def test_datanum_diff_span(self):
        # S1
        create_2stocks(operator=self._operator)
        self._ak_handler.obtain_data(para=self._short_para)
        self._rf_handler.reform_data()

        # S2
        create_ex_1stock(operator=self._operator, target=Target("000004"))
        para = Para().with_start_n_end(start=Date(2022, 1, 1), end=Date(2022, 7, 1))
        self._ak_handler.obtain_data(para=para)
        self._rf_handler.reform_data()

        assert dataframe_not_valid(self._rf_mtain.run(save=False))
