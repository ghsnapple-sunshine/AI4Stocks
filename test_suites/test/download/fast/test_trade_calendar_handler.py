import unittest

from buffett.download.fast import TradeCalendarHandler
from test.tester import Tester


class TradeCalendarHandlerTest(Tester):
    def test_download(self):
        hdl = TradeCalendarHandler(operator=self.operator)
        tbl = hdl._download()
        hdl._save_to_database(tbl)
        db = hdl.get_data()
        assert tbl.shape[0] == db.shape[0]


if __name__ == '__main__':
    unittest.main()
