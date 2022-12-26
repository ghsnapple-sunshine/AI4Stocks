from buffett.download.handler.list import BsStockListHandler
from buffett.download.handler.stock import ThDailyHandler
from test import Tester, create_1stock, DbSweeper


class TestThsDailyHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        target_list_handler = BsStockListHandler(cls._operator)
        cls._hdl = ThDailyHandler(
            cls._operator, target_list_handler=target_list_handler
        )

    def _setup_always(self):
        DbSweeper.erase()

    def test_download_20years(self):
        """
        测试能否正常下载

        :return:
        """
        create_1stock(self._operator, source="bs")
        self._hdl.obtain_data(para=self._great_para)

    def test_repeat_download(self):
        """
        测试重复下载

        :return:
        """
        create_1stock(self._operator, source="bs")
        self._hdl.obtain_data(para=self._great_para)
