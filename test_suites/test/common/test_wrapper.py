import unittest

from ai4stocks.common.wrapper import Wrapper, WrapperFactory


class InnerA:
    val = 1

    def run(self):
        return self.val


class InnerB:
    val = 1

    def run(self, obj):
        return self.val + obj


class InnerC:
    val = 1

    def run(self, obj=0):
        return self.val + obj


class WrapperTest(unittest.TestCase):
    def test_method_no_para(self):
        obj = InnerA()
        wrapper = WrapperFactory.from_method_name(obj, 'run')
        assert wrapper.run() == 1
        wrapper = WrapperFactory.from_method_ref(obj.run)
        assert wrapper.run() == 1

    def test_method_with_positional_para(self):
        obj = InnerB()
        wrapper = WrapperFactory.from_method_name(obj, 'run')
        assert wrapper.run(1) == 2
        wrapper = WrapperFactory.from_method_ref(obj.run)
        assert wrapper.run(1) == 2

    def test_method_with_optional_para(self):
        obj = InnerC()
        wrapper = WrapperFactory.from_method_name(obj, 'run')
        assert wrapper.run() == 1
        assert wrapper.run(obj=1) == 2
        wrapper = WrapperFactory.from_method_ref(obj.run)
        assert wrapper.run() == 1
        assert wrapper.run(obj=1) == 2

