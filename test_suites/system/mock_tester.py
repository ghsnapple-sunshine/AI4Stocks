from abc import abstractmethod

from buffett.adapter.pandas import DataFrame
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

    @staticmethod
    def copydb(table_name: str, meta: DataFrame, source: Operator, dest: Operator):
        df = source.select_data(name=table_name, meta=meta)
        dest.create_table(name=table_name, meta=meta)
        dest.insert_data(name=table_name, df=df)
