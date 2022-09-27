import unittest

from test.common.base_test import BaseTest


class TestMysqlConnect(BaseTest):
    def test_all(self):
        self.CreateTable()
        self.InsertData()
        self.SelectDataCnt()
        self.SelectData()
        self.SelectData2()
        self.DeleteData()
        self.DropTable()

    def CreateTable(self):
        sql = "create table `{0}` (name varchar(10), sex varchar(10), age int)".format(self.table_name)
        res = self.conn.Execute(sql)
        assert res == 0

    def InsertData(self):
        sql = "insert into `{0}` values(%s, %s, %s);".format(self.table_name)
        vals = [
            ['bravepuppy', 'female', 18],
            ['ghsnapple', 'male', 32],
            ['taylor', 'female', 9]
        ]
        res = self.conn.ExecuteMany(sql, vals, commit=True)
        assert res == 3

    def SelectDataCnt(self):
        sql = "select count(*) from `{0}`".format(self.table_name)
        res = self.conn.Execute(sql)
        assert res == 1

    def SelectData(self):
        sql = "select * from `{0}`".format(self.table_name)
        res = self.conn.Execute(sql)
        assert res == 3

    def SelectData2(self):
        sql = "select * from `{0}`".format(self.table_name)
        data = self.conn.Execute(sql, fetch=True)
        assert data.shape == (3, 3)
        assert data[data['name'] == 'bravepuppy']['sex'].iloc[0] == 'female'
        assert data[data['name'] == 'bravepuppy']['age'].iloc[0] == 18
        assert data[data['name'] == 'ghsnapple']['sex'].iloc[0] == 'male'
        assert data[data['name'] == 'ghsnapple']['age'].iloc[0] == 32
        assert data[data['name'] == 'taylor']['sex'].iloc[0] == 'female'
        assert data[data['name'] == 'taylor']['age'].iloc[0] == 9


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
