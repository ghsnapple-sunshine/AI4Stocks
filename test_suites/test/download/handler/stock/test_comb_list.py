from buffett.adapter.pandas import pd
from buffett.common.constants.col.target import CODE
from buffett.download.handler.list import (
    BsStockListHandler,
    SseStockListHandler,
    StockListHandler,
)
from test import Tester, DbSweeper


class TestStockListHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._comb_hdl = StockListHandler(operator=cls._operator)
        cls._bs_hdl = BsStockListHandler(operator=cls._operator)
        cls._ak_hdl = SseStockListHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        """
        测试AkStockList和BsStockList都往表格里写数据
        :return:
        """
        df1 = self._bs_hdl.obtain_data()
        df2 = self._ak_hdl.obtain_data()
        combine = pd.merge(df1, df2, how="outer", on=[CODE], suffixes=["_l", "_r"])
        DbSweeper.erase()
        self._comb_hdl.obtain_data()
        df3 = self._comb_hdl.select_data()
        assert df3.shape[0] == combine.shape[0]
