from buffett.common.interface import Singleton
from test import Tester


class TestSingleton(Tester):
    @classmethod
    def _setup_oncemore(cls):
        def init_method(s, v):
            super(s.__class__, s).__init__()
            s.v = v

        cls.SingletonSubClass = type(
            "SingletonSubClass", (Singleton,), {"__init__": init_method}
        )

    def _setup_always(self) -> None:
        pass

    def test_singleton(self):
        obj1 = Singleton()
        obj2 = Singleton()
        assert id(obj1) == id(obj2)
        assert type(obj1) == Singleton

    def test_singleton_subclass(self):
        obj1 = self.SingletonSubClass(1)
        obj2 = self.SingletonSubClass(2)
        assert id(obj1) == id(obj2)
        assert type(obj1) == self.SingletonSubClass
        """
        说明：
        执行obj2 = SingletonSubClass(2)时：
        1. 调用父类Singleton的__new__方法返回了Singleton的单例（也就是obj1),
        2. 调用子类SingletonSubClass的__init__方法中的self.v = v覆写了单例中的属性。
        3. obj1和obj2实为同一对象，其属性值均为2。
        """
        assert obj1.v == 2
        assert obj2.v == 2

    def test_all(self):
        self.test_singleton()
        self.test_singleton_subclass()
