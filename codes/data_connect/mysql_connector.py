from itertools import chain

import pymysql

from codes.data_connect.mysql_common import MysqlRole
from codes.tools.type_converter import TypeConverter


class MysqlConnector:
    def __init__(self, role):
        if role == MysqlRole.DbStock:
            self.user = 'stock'
            self.db = 'stocks'
        else:
            self.user = 'test'
            self.db = 'stockstest'
        self.password = 'Changeme_1234'
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

    def Disconnect(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            self.cursor.close()
            self.cursor = None

    def execute(self, sql, commit=False, fetch=False):
        self.connect()
        res = self.cursor.execute(sql)
        if commit:
            self.conn.commit()
        if fetch:
            res = self.cursor.fetchall()
            return TypeConverter.Tuple2Arr(res)
        return res

    def executeMany(self, sql, vals, commit=False):
        self.connect()
        res = self.cursor.executemany(sql, vals)
        if commit:
            self.conn.commit()
        return res
