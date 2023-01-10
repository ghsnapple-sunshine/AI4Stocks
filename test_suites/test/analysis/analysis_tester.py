from abc import abstractmethod

from buffett.analysis import Para
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from test import Tester


class AnalysisTester(Tester):
    _stk_rop = None
    _ana_rop = None
    _ana_wop = None

    @classmethod
    def _setup_once(cls):
        cls._stk_rop = Operator(RoleType.DB_TEST)
        cls._ana_rop = Operator(RoleType.DB_TEST)
        cls._ana_wop = Operator(RoleType.DB_TEST)
        cls._short_para = Para.from_base(cls._short_para)
        cls._long_para = Para.from_base(cls._long_para)
        cls._great_para = Para.from_base(cls._great_para)
        super(AnalysisTester, cls)._setup_once()

    @classmethod
    @abstractmethod
    def _setup_oncemore(cls):
        pass

    @abstractmethod
    def _setup_always(self) -> None:
        pass
