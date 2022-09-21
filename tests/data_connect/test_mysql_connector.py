import datetime
import unittest
from codes.data_connect.mysql_connector import MysqlConnector
from codes.data_connect.mysql_common import MysqlRole
from unittest import TestCase


class TestMysqlConnect(TestCase):
    def setUp(self):
        self.connector = MysqlConnector(MysqlRole.DbTest)
        self.connector.connect()
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.datatable = "test_{0}".format(now)

    def tearDown(self):
        self.dropTable()
        self.connector.Disconnect()

    def test_all(self):
        self.createTable()
        self.insertData()
        self.selectData()
        self.deleteData()
        self.dropTable()

    def createTable(self):
        sql = "create table `{0}` (name varchar(10), sex varchar(10), age int)".format(self.datatable)
        res = self.connector.execute(sql)
        assert res == 0

    def insertData(self):
        sql = "insert into `{0}` values(%s, %s, %s);".format(self.datatable)
        vals = [['bravapuppy', 'female', 18],
                ['ghsnapple', 'male', 33]]
        res = self.connector.executeMany(sql, vals, commit=True)
        assert res == 2

    def selectData(self):
        sql = "select * from `{0}`".format(self.datatable)
        res = self.connector.execute(sql)
        assert res == 2
        #sql2 = "select count(*) from ‘{0}’".format(self.datatable)
        #res = self.connector.execute(sql2)
        #assert res == 2

    def deleteData(self):
        sql = "delete from `{0}` where name = 'ghsnapple'".format(self.datatable)
        res = self.connector.execute(sql)
        assert res == 1

    def dropTable(self):
        sql = "drop table if exists `{0}`".format(self.datatable)
        res = self.connector.execute(sql)
        assert res == 0


if __name__ == '__main__':
    unittest.main()
