from buffett.common.wrapper import WrapperFactory
from test import SimpleTester


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


class WrapperTest(SimpleTester):
    def _setup_always(self) -> None:
        pass

    @classmethod
    def _setup_once(cls):
        pass

    @staticmethod
    def test_method_no_para():
        obj = InnerA()
        wrapper = WrapperFactory.from_method_name(obj, "run")
        assert wrapper.run() == 1
        wrapper = WrapperFactory.from_method_ref(obj.run)
        assert wrapper.run() == 1

    @staticmethod
    def test_method_with_positional_para():
        obj = InnerB()
        wrapper = WrapperFactory.from_method_name(obj, "run")
        assert wrapper.run(1) == 2
        wrapper = WrapperFactory.from_method_ref(obj.run)
        assert wrapper.run(1) == 2

    @staticmethod
    def test_method_with_optional_para():
        obj = InnerC()
        wrapper = WrapperFactory.from_method_name(obj, "run")
        assert wrapper.run() == 1
        assert wrapper.run(obj=1) == 2
        wrapper = WrapperFactory.from_method_ref(obj.run)
        assert wrapper.run() == 1
        assert wrapper.run(obj=1) == 2
