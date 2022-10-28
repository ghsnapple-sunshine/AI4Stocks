import unittest

from pendulum import DateTime

from ai4stocks.download.mysql.types import RoleType
from ai4stocks.download.mysql.operator import Operator
from test.common.db_sweeper import DbSweeper


class BaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.operator = Operator(role=RoleType.DbTest)
        self.operator.connect()
        self.conn = self.operator
        self.table_name = "test_{0}".format(
            DateTime.now().format('YYYYMMDD_HHmmss'))
        DbSweeper.cleanup()

    def tearDown(self) -> None:
        self.operator.disconnect()
