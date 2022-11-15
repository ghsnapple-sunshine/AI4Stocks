from buffett.common.tools import dataframe_not_valid
from buffett.download.fast.ak_profit_handler import AkProfitHandler
from test import Tester, DbSweeper


class AkProfitHandlerTest(Tester):
    def test_save(self):
        DbSweeper.cleanup()
        hdl = AkProfitHandler(operator=self.operator)
        df1 = hdl._download()
        hdl._save_to_database(df=df1)
        df2 = hdl.select_data()
        assert self.compare_dataframe(df1, df2)

    def test_repeat_download(self):
        DbSweeper.cleanup()
        hdl = AkProfitHandler(operator=self.operator)
        hdl.obtain_data()
        hdl.select_data()
        df2 = hdl._download()
        assert dataframe_not_valid(df2)
