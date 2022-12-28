from abc import abstractmethod

from buffett.analysis import Para
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from test import Tester


class AnalysisTester(Tester):
    _datasource_op = None

    @classmethod
    def _setup_once(cls):
        cls._datasource_op = Operator(RoleType.DbTest)
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
