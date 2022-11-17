from unittest import TestCase

from buffett.adapter.pandas import DataFrame, pd
from buffett.common.pendelum import DateTime
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from test import DbSweeper


class Tester(TestCase):
    def setUp(self) -> None:
        self.operator = Operator(role=RoleType.DbTest)
        self.operator.connect()
        self.conn = self.operator
        self.table_name = "test_{0}".format(DateTime.now().format("YYYYMMDD_HHmmss"))
        DbSweeper.cleanup()

    @staticmethod
    def compare_dataframe(df1: DataFrame, df2: DataFrame):
        """
        比较两个dataframe是否相同

        :param df1:
        :param df2:
        :return:
        """
        return pd.concat([df1, df2]).drop_duplicates(keep=False).empty

    def tearDown(self) -> None:
        self.operator.disconnect()
