from buffett.common.constants.col.target import INDUSTRY_CODE
from buffett.common.constants.table import INDUS_LS
from buffett.download.handler.industry import (
    AkIndustryListHandler,
    AkIndustryConsHandler,
)
from buffett.download.handler.list import StockListHandler
from test import Tester


class TestIndustryConsHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        StockListHandler(cls._operator).obtain_data()
        AkIndustryListHandler(cls._operator).obtain_data()
        cls._operator.execute(
            f"DELETE FROM {INDUS_LS} WHERE {INDUSTRY_CODE} > 'BK0450'"
        )  # 减少concept个数，提升测试性能
        cls._hdl = AkIndustryConsHandler(operator=cls._operator)

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
