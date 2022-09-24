import datetime
import unittest

from test.common.base_test import BaseTest


class TestMysqlConnect(BaseTest):
    def setUp(self):
        super().setUp()
        self.DropTable()

    def tearDown(self):
        self.DropTable()
        super().tearDown()

    def test_all(self):
        self.CreateTable()
        self.InsertData()
        self.SelectDataCnt()
        self.DeleteData()
        self.DropTable()

    def CreateTable(self):
        sql = "create table `{0}` (name varchar(10), sex varchar(10), age int)".format(self.table_name)
        res = self.conn.Execute(sql)
        assert res == 0

    def InsertData(self):
        sql = "insert into `{0}` values(%s, %s, %s);".format(self.table_name)
        vals = [['bravapuppy', 'female', 18],
                ['ghsnapple', 'male', 33]]
        res = self.conn.ExecuteMany(sql, vals, commit=True)
        assert res == 2

    def SelectDataCnt(self):
        sql = "select * from `{0}`".format(self.table_name)
        res = self.conn.Execute(sql)
        assert res == 2

    def DeleteData(self):
        sql = "delete from `{0}` where name = 'ghsnapple'".format(self.table_name)
        res = self.conn.Execute(sql)
        assert res == 1

    def DropTable(self):
        sql = "drop table if exists `{0}`".format(self.table_name)
        res = self.conn.Execute(sql)
        assert res == 0


if __name__ == '__main__':
    unittest.main()
