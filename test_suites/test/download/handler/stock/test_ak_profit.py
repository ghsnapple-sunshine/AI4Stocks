from buffett.common.constants.col import DATE
from buffett.common.constants.col.date import YEAR2025
from buffett.common.constants.col.target import CODE
from buffett.download.handler.stock import AkProfitHandler
from test import Tester


class AkProfitHandlerTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = AkProfitHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        self._download()
        self._repeat_download()

    def _download(self):
        df1 = self._hdl.obtain_data()
        df2 = self._hdl.select_data()
        del df2[YEAR2025]  # TODO:在2025年需调整该用例
        assert self.dataframe_almost_equals(df1, df2, join=[CODE, DATE])

    def _repeat_download(self):
        # 测试重复下载不报错
        self._hdl.obtain_data()
