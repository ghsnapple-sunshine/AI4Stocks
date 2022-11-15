from buffett.download.handler.stock import AkProfitHandler
from test import Tester


class AkProfitHandlerTest(Tester):
    def test_download(self):
        hdl = AkProfitHandler(operator=self.operator)
        df1 = hdl.obtain_data()
        df2 = hdl.select_data()
        assert self.compare_dataframe(df1, df2)

    def test_repeat_download(self):
        # 测试重复下载不报错
        AkProfitHandler(operator=self.operator).obtain_data()
