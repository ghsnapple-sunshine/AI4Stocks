from abc import abstractmethod

from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from test import Tester


class MockTester(Tester):
    _stk_op = None
    _ana_op = None

    @classmethod
    def _setup_once(cls):
        cls._stk_op = Operator(RoleType.DB_STK)
        cls._ana_op = Operator(RoleType.DB_ANA)
        super(MockTester, cls)._setup_once()

    @classmethod
    @abstractmethod
    def _setup_oncemore(cls):
        pass

    @abstractmethod
    def _setup_always(self) -> None:
        pass
