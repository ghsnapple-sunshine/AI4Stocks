import unittest

from buffett.download.fast.bs_money_supply_handler import BsMoneySupplyHandler
from test.tester import Tester


class BsMoneySupplyHandlerTest(Tester):
    def test_download(self):
        hdl = BsMoneySupplyHandler(operator=self.operator)
        tbl = hdl.obtain_data()
        db = hdl.select_data()
        assert tbl.shape[0] == db.shape[0]


if __name__ == '__main__':
    unittest.main()
