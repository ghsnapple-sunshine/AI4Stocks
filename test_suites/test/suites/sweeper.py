# 仅用于测试环境
from typing import Union

from buffett.adapter.pandas import Series
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType


class DbSweeper:
    _operator = Operator(RoleType.DbTest)

    @classmethod
    def cleanup(cls):
        """
        删除所有表

        :return:
        """
        names = cls._get_table_names()
        for n in names:
            cls._operator.drop_table(name=n)

    @classmethod
    def erase(cls):

        names = cls._get_table_names()
        for n in names:
            cls._operator.delete_data(name=n)

    @classmethod
    def cleanup_except(cls, excepts: Union[str, list[str]]):
        """
        删除所有表，除了例外

        :param excepts:
        :return:
        """
        table_names = cls._get_table_names()
        if isinstance(excepts, str):
            for n in table_names:
                if excepts != n:
                    cls._operator.drop_table(name=n)
        else:
            for n in table_names:
                if n not in excepts:
                    cls._operator.drop_table(name=n)

    @classmethod
    def erase_except(cls, excepts: Union[str, list[str]]):
        """
        清空所有表，除了例外

        :param excepts:
        :return:
        """
        table_names = cls._get_table_names()
        if isinstance(excepts, str):
            for n in table_names:
                if excepts != n:
                    cls._operator.delete_data(name=n)
        else:
            for n in table_names:
                if n not in excepts:
                    cls._operator.delete_data(name=n)

    @classmethod
    def _get_table_names(cls) -> Series:
        """
        获取数据库里所有的表名

        :return:
        """
        sql = "SHOW TABLES"
        db = cls._operator.execute(sql, fetch=True)
        if dataframe_not_valid(db):
            return Series(dtype=object)
        return db.iloc[:, 0]
