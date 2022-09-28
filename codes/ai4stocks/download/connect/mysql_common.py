from enum import Enum


class MysqlRole(Enum):
    DbStock = 1
    DbTest = 2
    DbInfo = 3
    ROOT = 4


class MysqlColType(Enum):
    STOCK_CODE = 1  # 股票代码（6位）
    LONG_STOCK_CODE = 2  # 股票代码（8位）
    STOCK_NAME = 3  # 股票名字
    FLOAT = 4
    INT32 = 5
    DATE = 10  # 日期
    DATETIME = 11  # 日期时间
    ENUM = 100  # 记录类型

    def ToSql(self):
        COL_TYPE_DICT = {
            MysqlColType.STOCK_CODE: 'VARCHAR(6)',
            MysqlColType.LONG_STOCK_CODE: 'VARCHAR(8)',
            MysqlColType.STOCK_NAME: 'VARCHAR(4)',
            MysqlColType.FLOAT: 'FLOAT',
            MysqlColType.INT32: 'INT',
            MysqlColType.DATE: 'DATE',
            MysqlColType.DATETIME: 'DATETIME',
            MysqlColType.ENUM: 'TINYINT'
        }
        return COL_TYPE_DICT[self]


class MysqlColAddReq(Enum):
    NONE = 1
    KEY = 2
    UNSIGNED = 3
    UNSIGNED_KEY = 4

    def ToSql(self):
        COL_ADDREQ_DICT = {
            MysqlColAddReq.NONE: '',
            MysqlColAddReq.KEY: 'NOT NULL',
            MysqlColAddReq.UNSIGNED: 'UNSIGNED',
            MysqlColAddReq.UNSIGNED_KEY: 'UNSIGNED NOT NULL'
        }
        return COL_ADDREQ_DICT[self]

    def IsKey(self):
        COL_ADDREQ_DICT = {
            MysqlColAddReq.NONE: False,
            MysqlColAddReq.KEY: True,
            MysqlColAddReq.UNSIGNED: False,
            MysqlColAddReq.UNSIGNED_KEY: True
        }
        return COL_ADDREQ_DICT[self]
