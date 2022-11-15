from buffett.constants.col.stock import INDUSTRY_CODE
from buffett.constants.table import IND_LS
from buffett.download.fast import StockListHandler, IndustryConsHandler, IndustryListHandler
from test import Tester


class TestIndustryConsHandler(Tester):
    def setUp(self) -> None:
        super(TestIndustryConsHandler, self).setUp()
        StockListHandler(self.operator).obtain_data()
        IndustryListHandler(self.operator).obtain_data()
        self.operator.execute(f"DELETE FROM {IND_LS} WHERE {INDUSTRY_CODE} > 'BK0450'")  # 减少concept个数，提升测试性能

    def test_download(self):
        hdl = IndustryConsHandler(operator=self.operator)
        df1 = hdl.obtain_data()
        df2 = hdl.select_data()
        assert self.compare_dataframe(df1, df2)

    def test_repeat_download(self):
        # 测试重复下载不报错
        IndustryConsHandler(operator=self.operator).obtain_data()
