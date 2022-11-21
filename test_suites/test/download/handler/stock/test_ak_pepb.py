from buffett.adapter.pandas import Series
from buffett.common import Code
from buffett.common.constants.col.stock import CODE
from buffett.download import Para
from buffett.download.handler.stock.ak_pepb import AkStockPePbHandler
from test import Tester, create_2stocks


class AkStockPePbHandlerTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = AkStockPePbHandler(operator=cls._operator)
        create_2stocks(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        self._download()
        self._repeat_download()

    def _download(self):
        self._hdl.obtain_data()
        df1 = self._hdl.select_data(para=Para().with_code(Code("000001")))
        df2 = self._hdl._download_and_save(Series({CODE: "000001"}))
        self.compare_dataframe(df1, df2)

    def _repeat_download(self):
        # 测试重复下载不报错
        self._hdl.obtain_data()
