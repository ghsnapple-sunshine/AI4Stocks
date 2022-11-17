from test import Tester


class TestConnector(Tester):
    def test_all(self):
        self._create_table()
        self._insert_data()
        self._select_data_cnt()
        self._select_data()
        self._select_data2()
        self._delete_data()
        self._drop_table()

    def _create_table(self):
        sql = "create table `{0}` (name varchar(10), sex varchar(10), age int)".format(
            self.table_name
        )
        res = self.conn.execute(sql)
        assert res == 0

    def _insert_data(self):
        sql = "insert into `{0}` values(%s, %s, %s);".format(self.table_name)
        vals = [
            ["bravepuppy", "female", 18],
            ["ghsnapple", "male", 32],
            ["taylor", "female", 9],
        ]
        res = self.conn.execute_many(sql, vals, commit=True)
        assert res == 3

    def _select_data_cnt(self):
        sql = "select count(*) from `{0}`".format(self.table_name)
        res = self.conn.execute(sql)
        assert res == 1

    def _select_data(self):
        sql = "select * from `{0}`".format(self.table_name)
        res = self.conn.execute(sql)
        assert res == 3

    def _select_data2(self):
        sql = "select * from `{0}`".format(self.table_name)
        data = self.conn.execute(sql, fetch=True)
        assert data.shape == (3, 3)
        assert data[data["name"] == "bravepuppy"]["sex"].iloc[0] == "female"
        assert data[data["name"] == "bravepuppy"]["age"].iloc[0] == 18
        assert data[data["name"] == "ghsnapple"]["sex"].iloc[0] == "male"
        assert data[data["name"] == "ghsnapple"]["age"].iloc[0] == 32
        assert data[data["name"] == "taylor"]["sex"].iloc[0] == "female"
        assert data[data["name"] == "taylor"]["age"].iloc[0] == 9

    def _delete_data(self):
        sql = "delete from `{0}` where name = 'ghsnapple'".format(self.table_name)
        res = self.conn.execute(sql)
        assert res == 1

    def _drop_table(self):
        sql = "drop table if exists `{0}`".format(self.table_name)
        res = self.conn.execute(sql)
        assert res == 0
