from __future__ import annotations

from ctypes import Union

from buffett.adapter.enum import Enum


class ColType(Enum):
    @classmethod
    def create(cls, col_type: str) -> ColType | str:
        COL_TYPE_DICT = {
            "VARCHAR(6)": 1,  # ColType.STOCK_CODE,
            "VARCHAR(8)": 2,  # ColType.STOCK_NAME
            "VARCHAR(10)": 3,  # ColType.CONCEPT_NAME,
            "VARCHAR(20)": 4,  # ColType.INDEX_NAME
            "FLOAT": 20,  # ColType.FLOAT
            "INT": 21,  # ColType.INT32
            "DATE": 10,  # ColType.DATE
            "DATETIME": 11,  # ColType.DATETIME
            "TINYINT UNSIGNED": 100,  # ColType.ENUM
            "VARCHAR(12)": 198,  # ColType.MINI_DESC
            "VARCHAR(100)": 199,  # ColType.SHORT_DESC
            "VARCHAR(10000)": 200,
        }  # ColType.LONG_DESC
        if col_type.upper() in COL_TYPE_DICT:
            return ColType(COL_TYPE_DICT[col_type.upper()])
        return col_type

    CODE = 1  # 股票代码（6位）
    STOCK_NAME = 2  # 股票名字（8位）
    CONCEPT_NAME = 3  # 概念名字（10位）
    INDEX_NAME = 4  # 指数名字（20位）

    FLOAT = 20
    INT32 = 21

    DATE = 10  # 日期
    DATETIME = 11  # 日期时间

    ENUM_BOOL = 100  # 枚举类型/Bool类型

    MINI_DESC = 198
    SHORT_DESC = 199
    LONG_DESC = 200

    def sql_format(self):
        COL_TYPE_DICT = {
            ColType.CODE: "VARCHAR(6)",
            ColType.STOCK_NAME: "VARCHAR(8)",
            ColType.CONCEPT_NAME: "VARCHAR(10)",
            ColType.INDEX_NAME: "VARCHAR(20)",
            ColType.FLOAT: "FLOAT",
            ColType.INT32: "INT",
            ColType.DATE: "DATE",
            ColType.DATETIME: "DATETIME",
            ColType.ENUM_BOOL: "TINYINT UNSIGNED",
            ColType.MINI_DESC: "VARCHAR(12)",
            ColType.SHORT_DESC: "VARCHAR(100)",
            ColType.LONG_DESC: "VARCHAR(10000)",
        }
        return COL_TYPE_DICT[self]
