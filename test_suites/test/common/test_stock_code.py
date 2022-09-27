import unittest

from ai4stocks.common.stock_code import StockCode


class TestStockCode(unittest.TestCase):
    def test_equal(self):
        cd1 = StockCode('000001')
        cd2 = StockCode('000001')
        assert cd1 == cd2

    def test_not_equal(self):
        cd1 = StockCode('000001')
        cd2 = StockCode('000002')
        assert cd1 != cd2


if __name__ == '__main__':
    unittest.main()
