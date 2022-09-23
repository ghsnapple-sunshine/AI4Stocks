from enum import Enum


class MysqlRole(Enum):
    DbStock = 1,
    DbTest = 2,
    DbInfo = 3


class MysqlColType(Enum):
    STOCK_CODE = 1,  # 股票代码（6位）
    STOCK_NAME = 2,  # 股票名字
    Float = 4,
    Int32 = 5,
    DATE = 10,  # 日期

    def toString(self):
        COLUMN_TYPE_DICT = {MysqlColType.STOCK_CODE: 'VARCHAR(6)',
                            MysqlColType.STOCK_NAME: 'VARCHAR(4)',
                            MysqlColType.Float: 'FLOAT',
                            MysqlColType.Int32: 'INT',
                            MysqlColType.DATE: 'VARCHAR(10)'}
        return COLUMN_TYPE_DICT[self]


class MysqlColAddReq(Enum):
    NONE = 1,
    PRIMKEY = 2

    def toString(self):
        COLUMNS_ADDREQ_DICT = {MysqlColAddReq.NONE: '',
                               MysqlColAddReq.PRIMKEY: 'PRIMARY KEY'}
        return COLUMNS_ADDREQ_DICT[self]


class MysqlConstants:
    META_COLS = ['column', 'type', 'addReq']
    STOCK_LIST_TABLE = 'stock_list'
    DAILY_INFO_TABLE = 'daily_info_{0}_{1}'
