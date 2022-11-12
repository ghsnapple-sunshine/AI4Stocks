import unittest

from buffett.common.tools import dataframe_not_valid
from buffett.download.fast import TradeCalendarHandler
from test import DbSweeper
from test.tester import Tester


class TradeCalendarHandlerTest(Tester):
    def test_download(self):
        DbSweeper.cleanup()
        hdl = TradeCalendarHandler(operator=self.operator)
        tbl = hdl._download()
        hdl._save_to_database(tbl)
        db = hdl.select_data()
        assert tbl.shape[0] == db.shape[0]

    def test_no_download(self):
        DbSweeper.cleanup()
        hdl = TradeCalendarHandler(operator=self.operator)
        db = hdl.select_data()
        assert dataframe_not_valid(db)


if __name__ == '__main__':
    unittest.main()
