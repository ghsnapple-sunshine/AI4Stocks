from buffett.common.constants.col.target import CODE, NAME
from buffett.download.handler.list import (
    BsStockListHandler,
)
from test import Tester


class TestBsStockListHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._bs_hdl = BsStockListHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        self._download()
        self._repeat_download()

    def _download(self):
        df1 = self._bs_hdl.obtain_data()
        df2 = self._bs_hdl.select_data()
        df1 = df1[[CODE, NAME]]
        df2 = df2[[CODE, NAME]]
        assert self.compare_dataframe(df1, df2)
        assert df1[CODE].apply(lambda x: x[0] != "4").all()

    def _repeat_download(self):
        before = self._bs_hdl.select_data().shape[0]
        self._bs_hdl.obtain_data()
        after = self._bs_hdl.select_data().shape[0]
        assert before == after
