import pymysql
from numpy import array
from pandas import DataFrame
from pymysql.err import DatabaseError, ProgrammingError, DataError

from buffett.download.mysql.types import RoleType


class Connector:
    def __init__(self, role):
        if role == RoleType.DbStock:
            self.user = 'stock'
            self.db = 'stocks'
            self.password = 'Changeme_1234'
        elif role == RoleType.DbTest:
            self.user = 'test'
            self.db = 'stockstest'
            self.password = 'Changeme_1234'
        elif role == RoleType.DbInfo:
            self.user = 'prosecutor'
            self.db = 'information_schema'
            self.password = 'Changeme_1234'
        elif role == RoleType.ROOT:
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

    def execute(self,
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

    def execute_many(self,
                     sql: str,
                     vals: array,
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
        except DataError as e:
            if e.args[0] == 1264:
                row = int(e.args[1].split(' ')[-1])
                print(sql)
                print(vals[row - 1: row + 1])
            raise e
        except DatabaseError as e:
            print(sql)
            raise e
