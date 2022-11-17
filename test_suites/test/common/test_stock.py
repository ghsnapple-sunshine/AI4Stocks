from buffett.common.stock import Code
from test import Tester


class TestStock(Tester):
    def test_equal(self):
        cd1 = Code("000001")
        cd2 = Code("000001")
        assert cd1 == cd2

    def test_not_equal(self):
        cd1 = Code("000001")
        cd2 = Code("000002")
        assert cd1 != cd2
