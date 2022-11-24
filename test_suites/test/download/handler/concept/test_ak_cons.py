from buffett.common.constants.col.stock import CONCEPT_CODE
from buffett.common.constants.table import CNCP_LS
from buffett.download.handler.concept import AkConceptConsHandler, AkConceptListHandler
from buffett.download.handler.list import StockListHandler
from test import Tester


class TestAkConceptConsHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        StockListHandler(cls._operator).obtain_data()
        AkConceptListHandler(cls._operator).obtain_data()
        cls._operator.execute(
            f"DELETE FROM {CNCP_LS} WHERE {CONCEPT_CODE} > 'BK0500'", commit=True
        )  # 减少concept个数，提升测试性能
        cls._hdl = AkConceptConsHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        self._download()
        self._repeat_download()

    def _download(self):
        df1 = self._hdl.obtain_data()
        df2 = self._hdl.select_data()
        assert self.compare_dataframe(df1, df2)

    def _repeat_download(self):
        # 测试重复下载不报错
        self._hdl.obtain_data()
