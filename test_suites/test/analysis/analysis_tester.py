from abc import abstractmethod

from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from test import Tester


class AnalysisTester(Tester):
    _insert_op = None
    _select_op = None

    @classmethod
    def _setup_once(cls):
        cls._select_op = Operator(RoleType.DbTest)
        cls._insert_op = Operator(RoleType.DbTest)  # 两个线程不能使用相同的Operator
        super(AnalysisTester, cls)._setup_once()

    @classmethod
    @abstractmethod
    def _setup_oncemore(cls):
        pass

    @abstractmethod
    def _setup_always(self) -> None:
        pass
