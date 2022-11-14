from __future__ import annotations

from buffett.adapter.enum import Enum


class ColType(Enum):
    @classmethod
    def create(cls, col_type: str) -> ColType:
        COL_TYPE_DICT = {'VARCHAR(6)': 1,  # ColType.STOCK_CODE_NAME,
                         'VARCHAR(8)': 2,  # ColType.LONG_STOCK_CODE,
                         'FLOAT': 4,  # ColType.FLOAT
                         'INT': 5,  # ColType.INT32
                         'DATE': 10,  # ColType.DATE
                         'DATETIME': 11,  # ColType.DATETIME
                         'TINYINT UNSIGNED': 100,  # ColType.ENUM
                         'VARCHAR(100): 199'  # ColType.SHORT_DESC
                         'VARCHAR(10000)': 200}  # ColType.LONG_DESC
        return ColType(COL_TYPE_DICT[col_type.upper()])

    STOCK_CODE_NAME = 1  # 股票代码（6位）/股票名字
    LONG_STOCK_CODE = 2  # 股票代码（8位）
    FLOAT = 4
    INT32 = 5
    DATE = 10  # 日期
    DATETIME = 11  # 日期时间
    ENUM_BOOL = 100  # 枚举类型/Bool类型
    SHORT_DESC = 199
    LONG_DESC = 200

    def sql_format(self):
        COL_TYPE_DICT = {ColType.STOCK_CODE_NAME: 'VARCHAR(6)',
                         ColType.LONG_STOCK_CODE: 'VARCHAR(8)',
                         ColType.FLOAT: 'FLOAT',
                         ColType.INT32: 'INT',
                         ColType.DATE: 'DATE',
                         ColType.DATETIME: 'DATETIME',
                         ColType.ENUM_BOOL: 'TINYINT UNSIGNED',
                         ColType.SHORT_DESC: 'VARCHAR(100)',
                         ColType.LONG_DESC: 'VARCHAR(10000)'}
        return COL_TYPE_DICT[self]
