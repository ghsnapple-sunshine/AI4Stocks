from buffett.download.fast import BsMoneySupplyHandler
from test import Tester


class BsMoneySupplyHandlerTest(Tester):
    def test_download(self):
        # 货币供应数值有精度问题，暂时只考虑比较写入行数
        hdl = BsMoneySupplyHandler(operator=self.operator)
        df1 = hdl.obtain_data()
        df2 = hdl.select_data()
        assert df1.shape[0] == df2.shape[0]
