import unittest

from pendulum import DateTime

from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from test.common.db_sweeper import DbSweeper


class BaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.op = MysqlOperator(role=MysqlRole.DbTest)
        self.op.connect()
        self.conn = self.op
        self.table_name = "test_{0}".format(
            DateTime.now().format('YYYYMMDD_HHmmss'))
        DbSweeper.cleanup()

    def tearDown(self) -> None:
        self.op.disconnect()
