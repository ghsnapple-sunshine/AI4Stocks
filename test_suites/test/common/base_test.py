import unittest

from ai4stocks.data_connect.mysql_common import MysqlRole
from ai4stocks.data_connect.mysql_operator import MysqlOperator


class BaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.op = MysqlOperator(role=MysqlRole.DbTest)
        self.op.Connect()
        self.conn = self.op

    def tearDown(self) -> None:
        self.op.Disconnect()
