from buffett.common.constants.col.target import CODE
from buffett.download.handler.list.ak_list import StockListHandler
from test import Tester, DbSweeper


class StockListHandlerTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = StockListHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        self._download()
        self._repeat_download()

    def _download(self):
        df1 = self._hdl.obtain_data()
        df2 = self._hdl.select_data()
        assert self.compare_dataframe(df1, df2)
        assert df1[CODE].apply(lambda x: x[0] != "4").all()

    def _repeat_download(self):
        self._hdl.obtain_data()
