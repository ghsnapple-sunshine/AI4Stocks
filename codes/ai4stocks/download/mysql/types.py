from enum import Enum


class RoleType(Enum):
    DbStock = 1
    DbTest = 2
    DbInfo = 3
    ROOT = 4


class ColType(Enum):
    STOCK_CODE = 1  # 股票代码（6位）
    LONG_STOCK_CODE = 2  # 股票代码（8位）
    STOCK_NAME = 3  # 股票名字
    FLOAT = 4
    INT32 = 5
    DATE = 10  # 日期
    DATETIME = 11  # 日期时间
    ENUM = 100  # 记录类型

    def sql_format(self):
        COL_TYPE_DICT = {
            ColType.STOCK_CODE: 'VARCHAR(6)',
            ColType.LONG_STOCK_CODE: 'VARCHAR(8)',
            ColType.STOCK_NAME: 'VARCHAR(4)',
            ColType.FLOAT: 'FLOAT',
            ColType.INT32: 'INT',
            ColType.DATE: 'DATE',
            ColType.DATETIME: 'DATETIME',
            ColType.ENUM: 'TINYINT'
        }
        return COL_TYPE_DICT[self]


class AddReqType(Enum):
    NONE = 1
    KEY = 2
    UNSIGNED = 3
    UNSIGNED_KEY = 4

    def sql_format(self):
        COL_ADDREQ_DICT = {
            AddReqType.NONE: '',
            AddReqType.KEY: 'NOT NULL',
            AddReqType.UNSIGNED: 'UNSIGNED',
            AddReqType.UNSIGNED_KEY: 'UNSIGNED NOT NULL'
        }
        return COL_ADDREQ_DICT[self]

    def is_key(self):
        COL_ADDREQ_DICT = {
            AddReqType.NONE: False,
            AddReqType.KEY: True,
            AddReqType.UNSIGNED: False,
            AddReqType.UNSIGNED_KEY: True
        }
        return COL_ADDREQ_DICT[self]
