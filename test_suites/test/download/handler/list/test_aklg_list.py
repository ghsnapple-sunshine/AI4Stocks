from buffett.common.constants.col.stock import CODE
from buffett.download.handler.list.ak_lg_list import AkLgStockListHandler
from test import Tester


class AkLgStockListHandlerTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = AkLgStockListHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        self._download()
        self._repeat_download()

    def _download(self):
        df1 = self._hdl.obtain_data()
        df2 = self._hdl.select_data()
        assert self.dataframe_equals(df1, df2)
        assert df1[CODE].apply(lambda x: x[0] != "4").all()

    def _repeat_download(self):
        self._hdl.obtain_data()
