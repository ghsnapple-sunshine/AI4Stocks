from abc import abstractmethod
from unittest import TestCase

from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.pendulum import DateTime
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType


class SingletonTester(TestCase):
    def __init__(self, methodName="runTest"):
        super(SingletonTester, self).__init__(methodName=methodName)
        if hasattr(self, "_prepared") and self._prepared:
            return
        self._operator = Operator(role=RoleType.DbTest)
        self._operator.connect()
        self._table_name = "test_{0}".format(DateTime.now().format("YYYYMMDD_HHmmss"))
        self._more_init()
        self._prepared = True

    @abstractmethod
    def _more_init(self):
        pass

    @staticmethod
    def compare_dataframe(df1: DataFrame, df2: DataFrame):
        """
        比较两个dataframe是否相同

        :param df1:
        :param df2:
        :return:
        """
        return pd.concat([df1, df2]).drop_duplicates(keep=False).empty

    def __del__(self):
        self._operator.disconnect()
