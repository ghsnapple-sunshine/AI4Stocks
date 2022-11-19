# 仅用于测试环境

from buffett.adapter.pandas import Series
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType


class DbSweeper:
    _op = Operator(RoleType.DbTest)

    @classmethod
    def cleanup(cls):
        """
        删除所有表

        :return:
        """
        names = cls._get_table_names()
        for n in names:
            cls._op.drop_table(name=n)

    @classmethod
    def erase(cls):
        """
        清除所有表

        :return:
        """
        names = cls._get_table_names()
        for n in names:
            cls._op.delete_data(name=n)

    @classmethod
    def cleanup_except(cls, name: str):
        """
        删除所有表，除了例外

        :return:
        """
        names = cls._get_table_names()
        for n in names:
            if name != n:
                cls._op.drop_table(name=n)

    @classmethod
    def erase_except(cls, name: str):
        """
        清空所有表，除了例外

        :return:
        """
        names = cls._get_table_names()
        for n in names:
            if name != n:
                cls._op.delete_data(name=n)

    @classmethod
    def _get_table_names(cls) -> Series:
        """
        获取数据库里所有的表名

        :return:
        """
        sql = "SHOW TABLES"
        db = cls._op.execute(sql, fetch=True)
        if dataframe_not_valid(db):
            return Series()
        return db.iloc[:, 0]
