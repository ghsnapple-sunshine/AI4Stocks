from typing import Any

from buffett.adapter.pandas import DataFrame
from buffett.adapter.pymysql import connect, DatabaseError, ProgrammingError, DataError
from buffett.common.logger import Logger, LoggerBuilder
from buffett.download.mysql.types import RoleType


class Connector:
    def __init__(self, role):
        # self._role = role
        if role == RoleType.DbStock:
            self._user = "target"
            self._db = "stocks"
            self._pwd = "Changeme_1234"
        elif role == RoleType.DbTest:
            self._user = "test"
            self._db = "stockstest"
            self._pwd = "Changeme_1234"
        elif role == RoleType.DbInfo:
            self._user = "prosecutor"
            self._db = "information_schema"
            self._pwd = "Changeme_1234"
        elif role == RoleType.ROOT:
            self._user = "root"
            self._db = "stocks"
            self._pwd = input("请输入root@localhost的密码")

        self._conn = None
        self._cursor = None
        self._logger: ConnectorLogger = LoggerBuilder.build(ConnectorLogger)()

    def connect(self):
        if self._conn is None:
            config = {
                "host": "127.0.0.1",
                "port": 3306,
                "user": self._user,
                "passwd": self._pwd,
                "db": self._db,
                "charset": "utf8mb4",
            }
            self._conn = connect(**config)
            self._cursor = self._conn.cursor()

    def disconnect(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._cursor.close()
            self._cursor = None

    def execute(self, sql: str, fetch: bool = False, commit: bool = True):
        """
        执行sql

        :param sql:
        :param fetch:
        :param commit:
        :return:
        """
        self.connect()
        try:
            self._logger.info_print_sql(sql)
            res = self._cursor.execute(sql)
            if commit:
                self._conn.commit()
            if fetch:
                res = self._cursor.fetchall()
                db = DataFrame(list(res))
                if not db.empty:
                    db.columns = [d[0] for d in self._cursor.description]
                return db
            return res
        except ProgrammingError as e:
            if e.args[0] == 1146:
                return
            else:
                raise e
        except DatabaseError as e:
            raise e

    def execute_many(self, sql: str, vals: list[list[Any]], commit: bool = True):
        self.connect()
        try:
            self._logger.info_print_sql(sql)
            res = self._cursor.executemany(sql, vals)
            if commit:
                self._conn.commit()
            return res
        except ProgrammingError as e:
            self._conn.rollback()
            self._logger.error_print_msg(e)
            if e.args[0] == 1146:
                return
            else:
                print(sql)
                raise e
        except DataError as e:
            self._conn.rollback()
            self._logger.error_print_msg(e)
            if e.args[0] in [1264, 1406]:
                row = int(e.args[1].split(" ")[-1])
                print(vals[row - 1 : row + 1])
            raise e
        except DatabaseError as e:
            self._conn.rollback()
            self._logger.error_print_msg(e)
            raise e


class ConnectorLogger(Logger):
    def info_print_sql(self, sql: str):
        self.debug(sql)

    def error_print_msg(self, e: Exception):
        self.error(str(e))
