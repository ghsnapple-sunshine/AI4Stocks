import unittest

from ai4stocks.download.fast import TradeCalendarHandler
from test.common.base_test import BaseTest


class TradeCalendarHandlerTest(BaseTest):
    def test_download(self):
        hdl = TradeCalendarHandler(operator=self.op)
        tbl = hdl.__download__()
        hdl.__save_to_database__(tbl)
        db = hdl.get_table()
        assert tbl.shape[0] == db.shape[0]


if __name__ == '__main__':
    unittest.main()
