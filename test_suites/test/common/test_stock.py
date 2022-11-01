import unittest

from buffett.common.stock import Code


class TestStock(unittest.TestCase):
    def test_equal(self):
        cd1 = Code('000001')
        cd2 = Code('000001')
        assert cd1 == cd2

    def test_not_equal(self):
        cd1 = Code('000001')
        cd2 = Code('000002')
        assert cd1 != cd2


if __name__ == '__main__':
    unittest.main()
