from typing import Any

from buffett.adapter.enum import Enum
from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.pendulum import date
from buffett.common.constants.col.meta import ADDREQ, COLUMN


class InsertSqlParser:
    @staticmethod
    def insert(name: str,
               df: DataFrame,
               ignore: bool = False) -> tuple[str, list[list[Any]]]:
        """
        try into or try ignore into

        :param name:
        :param df:
        :param ignore:      if true: use 'insert ignore into', otherwise use 'insert into'
        :return:
        """
        sql = "insert {3}into `{0}`({1}) values({2})".format(
            name,
            ', '.join([f'`{x}`' for x in df.columns]),
            ', '.join(['%s'] * df.columns.size),
            'ignore ' if ignore else '')
        vals = InsertSqlParser._get_format_list(df)  # 应用过obj_format处无需再df.replace
        return sql, vals

    @staticmethod
    def insert_n_update(name,
                        df: DataFrame,
                        meta: DataFrame):
        """
        insert into on duplicate key update

        :param name:
        :param df:
        :param meta:
        :return:
        """
        normal_cols = meta[meta.apply(lambda x: x[ADDREQ].not_key() and x[COLUMN] in df.columns, axis=1)][COLUMN]
        inQ2 = [f'`{x}`=values(`{x}`)' for x in normal_cols]

        sql = "insert into `{0}`({1}) values({2}) on duplicate key update {3}".format(
            name,
            ', '.join([f'`{x}`' for x in df.columns]),  # 避免data.columns中有非str类型导致报错
            ', '.join(['%s'] * df.columns.size),
            ', '.join(inQ2))
        vals = InsertSqlParser._get_format_list(df)  # 应用过obj_format处无需再df.replace
        return sql, vals

    @staticmethod
    def _get_format_list(df: DataFrame) -> list[list[Any]]:
        """
        将dataframe转成可被pymsql写入的list

        :param df:
        :return:
        """
        return [[InsertSqlParser._obj_format(y) for y in x] for x in df.values]

    @staticmethod
    def _obj_format(obj: Any) -> Any:
        """
        将对象转换成可被sql插入的格式

        :param obj:             待插入的对象
        :return:
        """
        if pd.isna(obj):
            return None
        if isinstance(obj, Enum):
            return str(obj.value)
        if isinstance(obj, date):
            return str(obj)
        return obj
