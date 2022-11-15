from buffett.download.handler.industry import IndustryListHandler
from test import Tester


class TestConceptListHandler(Tester):
    def test_download(self):
        hdl = IndustryListHandler(operator=self.operator)
        df1 = hdl.obtain_data()
        df2 = hdl.select_data()
        assert self.compare_dataframe(df1, df2)

    def test_repeat_download(self):
        # 测试重复下载不报错
        IndustryListHandler(operator=self.operator).obtain_data()
