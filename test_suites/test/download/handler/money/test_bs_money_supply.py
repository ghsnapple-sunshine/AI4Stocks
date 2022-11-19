from buffett.download.handler.money import BsMoneySupplyHandler
from test import Tester


class BsMoneySupplyHandlerTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = BsMoneySupplyHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        self._download()
        self._repeat_download()

    def _download(self):
        # 货币供应数值有精度问题，暂时只考虑比较写入行数
        df1 = self._hdl.obtain_data()
        df2 = self._hdl.select_data()
        assert df1.shape[0] == df2.shape[0]

    def _repeat_download(self):
        # 测试重复下载不报错
        self._hdl.obtain_data()
