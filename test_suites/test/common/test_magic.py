from buffett.adapter.akshare import ak_stock_info_a_code_name, ak_stock_zh_a_hist, ak
from buffett.common.magic.tools import get_func_params
from test.suites.acc import Accelerator
from test import SimpleTester


class TestMagic(SimpleTester):
    @classmethod
    def _setup_once(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_get_func_with_3params_2defaults(self):
        def func2(a, b=None, c=3):
            pass

        actual = get_func_params(func2)
        expectation = [["a", None], ["b", None], ["c", 3]]
        assert actual == expectation

    def test_get_func_with_3params_1defaults(self):
        def func2(a, b, c=3):
            pass

        actual = get_func_params(func2)
        expectation = [["a", None], ["b", None], ["c", 3]]
        assert actual == expectation

    def test_get_func_with_0params(self):
        def func2():
            pass

        actual = get_func_params(func2)
        expectation = []
        assert actual == expectation

    def test_get_func_with_2params(self):
        def func2(a, b):
            pass

        actual = get_func_params(func2)
        expectation = [["a", None], ["b", None]]
        assert actual == expectation

    def test_get_boundmethod_with_2params(self):
        class InnerA:
            def print(self, a, b):
                pass

        actual = get_func_params(InnerA().print)
        expectation = [["a", None], ["b", None]]
        assert actual == expectation

    def test_get_classmethod_with_0param(self):
        class InnerA:
            @classmethod
            def print(cls):
                pass

        actual = get_func_params(InnerA().print)
        expectation = []
        assert actual == expectation

    def test_get_staticmethod_with_1param(self):
        class InnerA:
            @staticmethod
            def print(cls):
                pass

        actual = get_func_params(InnerA().print)
        expectation = [["cls", None]]
        assert actual == expectation

    def test_get_akshare_func_0param(self):
        func = Accelerator.restore(ak.stock_info_a_code_name)
        if func is None:
            func = ak.stock_info_a_code_name
        actual = get_func_params(func)
        expectation = []
        assert actual == expectation

    def test_get_akshare_func_5params(self):
        func = Accelerator.restore(ak.stock_zh_a_hist)
        if func is None:
            func = ak.stock_zh_a_hist
        actual = get_func_params(func)
        expectation = [
            ["symbol", None],
            ["period", None],
            ["start_date", None],
            ["end_date", None],
            ["adjust", None],
        ]
        assert actual == expectation
