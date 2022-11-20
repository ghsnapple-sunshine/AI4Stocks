from buffett.common.stock import Code
from test import SimpleTester


class TestStock(SimpleTester):
    @classmethod
    def _setup_once(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_code_equal(self):
        cd1 = Code("000001")
        cd2 = Code("000001")
        assert cd1 == cd2

    def test_code_not_equal(self):
        cd1 = Code("000001")
        cd2 = Code("000002")
        assert cd1 != cd2
