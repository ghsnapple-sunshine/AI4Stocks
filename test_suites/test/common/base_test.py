import unittest

from ai4stocks.download.data_connect.mysql_common import MysqlRole
from ai4stocks.download.data_connect.mysql_operator import MysqlOperator
from pendulum import DateTime


class BaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.op = MysqlOperator(role=MysqlRole.DbTest)
        self.op.Connect()
        self.conn = self.op
        self.table_name = "test_{0}".format(DateTime.now().format('YYYYMMDD_hhmmss'))

    def tearDown(self) -> None:
        self.op.Disconnect()
