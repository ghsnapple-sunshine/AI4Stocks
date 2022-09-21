from enum import Enum


class MysqlRole(Enum):
    DbStock = 1,
    DbTest = 2


class MysqlColumnType(Enum):
    Varchar8 = 1, # 股票代码（8位）
    Varchar6 = 2, # 股票代码（6位）
    Varchar4 = 3, # 股票名字（4位）
    Float = 4


class MysqlColumnAddReq(Enum):
    NONE = 1,
    PRIMKEY = 2


class MysqlConstants:
    COLUMN_INDEXS = ['column', 'type', 'addReq']
    COLUMN_TYPE_DICT = {MysqlColumnType.Varchar8: 'VARCHAR(8)',
                        MysqlColumnType.Varchar6: 'VARCHAR(6)',
                        MysqlColumnType.Varchar4: 'VARCHAR(4)',
                        MysqlColumnType.Float: 'FLOAT'}
    COLUMNS_ADDREQ_DICT = {MysqlColumnAddReq.NONE: '',
                           MysqlColumnAddReq.PRIMKEY: 'PRIMARY KEY'}
    STOCK_LIST_TABLE = 'stock_list'

