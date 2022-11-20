from buffett.adapter.akshare import ak
from buffett.adapter.pendulum import DateTime
from test.suites.acc import Accelerator
from test import SimpleTester


class TestAccelerator(SimpleTester):
    @classmethod
    def _setup_once(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_repeat_download_stock_list(self):
        ak.stock_info_a_code_name()
        start = DateTime.now()
        self._atom_download()
        duration = DateTime.now() - start
        func = Accelerator.restore(ak.stock_info_a_code_name)
        assert (func,) in Accelerator._cache
        assert duration.total_seconds() < 0.1

    def test_signature(self):
        def func2(a, b, c, d=2, e=3):
            return a + b + c + d + e

        func = Accelerator(func2).mock()
        key1 = (func2, 1, 2, 3, 2, 5)
        result1 = func(b=2, a=1, c=3, e=5)
        key2 = (func2, 0, -2, 5, 1, 4)
        result2 = func(a=0, b=-2, c=5, d=1, e=4)
        assert key1 in Accelerator._cache
        assert Accelerator._cache[key1] == result1
        assert key2 in Accelerator._cache
        assert Accelerator._cache[key2] == result2

    @staticmethod
    def _atom_download():
        ak.stock_info_a_code_name = Accelerator(ak.stock_info_a_code_name).mock()
        for x in range(0, 10):
            ak.stock_info_a_code_name()
