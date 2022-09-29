import pymysql
from pandas import DataFrame

from ai4stocks.download.connect.mysql_common import MysqlRole
from pymysql.err import DatabaseError, ProgrammingError


class MysqlConnector:
    def __init__(self, role):
        if role == MysqlRole.DbStock:
            self.user = 'stock'
            self.db = 'stocks'
            self.password = 'Changeme_1234'
        elif role == MysqlRole.DbTest:
            self.user = 'test'
            self.db = 'stockstest'
            self.password = 'Changeme_1234'
        elif role == MysqlRole.DbInfo:
            self.user = 'prosecutor'
            self.db = 'information_schema'
            self.password = 'Changeme_1234'
        elif role == MysqlRole.ROOT:
            self.user = 'root'
            self.db = 'stocks'
            self.password = input('请输入root@localhost的密码')

        self.conn = None
        self.cursor = None

    def connect(self):
        if self.conn is None:
            config = {'host': '127.0.0.1',
                      'port': 3306,
                      'user': self.user,
                      'passwd': self.password,
                      'db': self.db,
                      'charset': 'utf8mb4'}
            self.conn = pymysql.connect(**config)
            self.cursor = self.conn.cursor()

    def disconnect(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            self.cursor.close()
            self.cursor = None

    def execute(
            self,
            sql: str,
            commit: bool = False,
            fetch: bool = False):
        self.connect()
        try:
            res = self.cursor.execute(sql)
            if commit:
                self.conn.commit()
            if fetch:
                res = self.cursor.fetchall()
                db = DataFrame(list(res))
                if not db.empty:
                    db.columns = [d[0] for d in self.cursor.description]
                return db
            return res
        except ProgrammingError as e:
            if e.args[0] == 1146:
                return DataFrame()
            else:
                print(sql)
                raise e
        except DatabaseError as e:
            print(sql)
            raise e

    def execute_many(
            self,
            sql: str,
            vals: list,
            commit: bool = False):
        self.connect()
        try:
            res = self.cursor.executemany(sql, vals)
            if commit:
                self.conn.commit()
            return res
        except ProgrammingError as e:
            if e.args[0] == 1146:
                return DataFrame()
            else:
                print(sql)
                raise e
        except DatabaseError as e:
            print(sql)
            print(vals[0])
            raise e
