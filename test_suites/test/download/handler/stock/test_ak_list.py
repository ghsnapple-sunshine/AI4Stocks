from buffett.constants.col.stock import CODE
from buffett.download.handler.stock.ak_list import StockListHandler
from test import Tester


class StockListHandlerTest(Tester):
    def test_download_result(self):
        hdl = StockListHandler(operator=self.operator)
        df1 = hdl.obtain_data()
        df2 = hdl.select_data()
        assert self.compare_dataframe(df1, df2)
        assert df1[CODE].apply(lambda x: x[0] != '4').all()

    def test_repeat_download(self):
        StockListHandler(operator=self.operator).obtain_data()
