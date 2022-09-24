from enum import Enum


class MysqlRole(Enum):
    DbStock = 1,
    DbTest = 2,
    DbInfo = 3


class MysqlColType(Enum):
    STOCK_CODE = 1,  # 股票代码（6位）
    LONG_STOCK_CODE = 2,  # 股票代码（8位）
    STOCK_NAME = 3,  # 股票名字
    Float = 4,
    Int32 = 5,
    DATE = 10,  # 日期
    DATETIME = 11  # 日期时间

    def toString(self):
        COL_TYPE_DICT = {MysqlColType.STOCK_CODE: 'VARCHAR(6)',
                         MysqlColType.LONG_STOCK_CODE: 'VARCHAR(8)',
                         MysqlColType.STOCK_NAME: 'VARCHAR(4)',
                         MysqlColType.Float: 'FLOAT',
                         MysqlColType.Int32: 'INT',
                         MysqlColType.DATE: 'DATE',
                         MysqlColType.DATETIME: 'DATETIME'}
        return COL_TYPE_DICT[self]


class MysqlColAddReq(Enum):
    NONE = 1,
    PRIMKEY = 2

    def toString(self):
        COL_ADDREQ_DICT = {MysqlColAddReq.NONE: '',
                           MysqlColAddReq.PRIMKEY: 'PRIMARY KEY'}
        return COL_ADDREQ_DICT[self]


class MysqlConstants:
    META_COLS = ['column', 'type', 'addReq']
    STOCK_LIST_TABLE = 'stock_list'
    DAILY_INFO_TABLE = 'daily_info_{0}_{1}'
    MINUTE_INFO_TABLE = 'minute{0}_info_{1}_{2}'
