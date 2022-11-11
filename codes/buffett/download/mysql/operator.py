from datetime import date
from enum import Enum
from typing import Optional, Union, Any

import pandas as pd
from pandas import DataFrame

from buffett.common.pendelum import DateSpan
from buffett.common.tools import dataframe_not_valid, list_not_valid
from buffett.constants.col import DATE, DATETIME
from buffett.constants.dbcol import FIELD
from buffett.constants.meta import COLUMN, TYPE, ADDREQ
from buffett.download.mysql.connector import Connector
from buffett.download.mysql.reqcol import ReqCol


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


def _dataframe_2_format_list(df: DataFrame) -> list[list[Any]]:
    """
    将dataframe转成可被pymsql写入的list

    :param df:
    :return:
    """
    return [[_obj_format(y) for y in x] for x in df.values]


def _groupby_ext(groupby: list[ReqCol]):
    """
    扩展sql：groupby

    :param groupby:         需要groupby的列
    :return:
    """
    if len(groupby) == 0:
        return ''

    sql = ','.join([x.simple_format() for x in groupby])
    sql = f'group by {sql}'
    return sql


class Operator(Connector):
    def create_table(self,
                     name: str,
                     meta: DataFrame,
                     if_not_exist=True) -> None:
        """
        在Mysql中创建表

        :param name:                表名
        :param meta:                表元数据
        :param if_not_exist:        检查表是否存在，不存在则创建
        :return:
        """
        """
        cols = []
        for index, row in meta.iterrows():
            s = '{0} {1} {2}'.format(row[COLUMN], row[TYPE].sql_format(), row[ADDREQ].sql_format())
            cols.append(s)
        """
        cols = [f'`{row[COLUMN]}` {row[TYPE].sql_format()} {row[ADDREQ].sql_format()}'
                for index, row in meta.iterrows()]
        prim_key = meta[meta[ADDREQ].apply(lambda x: x.is_key())][COLUMN]

        if not prim_key.empty:
            apd = ','.join(prim_key)
            apd = 'primary key ({0})'.format(apd)
            cols.append(apd)

        joinCols = ','.join(cols)
        str_if_exist = 'if not exists ' if if_not_exist else ''
        sql = 'create table {0}`{1}` ({2})'.format(str_if_exist, name, joinCols)
        self.execute(sql)

    def insert_data(self,
                    name: str,
                    df: DataFrame) -> None:
        """
        插入数据到Mysql表

        :param name:                表名
        :param df:                  数据
        :return:
        """
        if dataframe_not_valid(df):
            return

        sql = "insert into `{0}`({1}) values({2})".format(
            name,
            ', '.join([f'`{x}`' for x in df.columns]),
            ', '.join(['%s'] * df.columns.size))
        vals = _dataframe_2_format_list(df)  # 应用过obj_format处无需再df.replace
        self.execute_many(sql, vals, True)

    def try_insert_data(self,
                        name: str,
                        df: DataFrame,
                        meta: DataFrame = None,
                        update: bool = False) -> None:
        """
        尝试插入数据到Mysql表

        :param name:                表名
        :param df:                  数据
        :param meta:                表元数据
        :param update:              True: 插入失败则更新; False: 插入失败则跳过
        :return:
        """
        if dataframe_not_valid(df):
            return

        if update:
            self._try_insert_n_update_data_ex(name=name, df=df, meta=meta)
            return

        sql = "insert ignore into `{0}`({1}) values({2})".format(
            name,
            ', '.join([f'`{x}`' for x in df.columns]),
            ', '.join(['%s'] * df.columns.size))
        vals = _dataframe_2_format_list(df)  # 应用过obj_format处无需再df.replace
        self.execute_many(sql, vals, True)

    def _try_insert_n_update_data_ex(self,
                                     name: str,
                                     df: DataFrame,
                                     meta: DataFrame) -> None:
        """
        尝试插入数据到Mysql表（插入失败则更新）(性能优化版）

        :param name:                表名
        :param df:                  数据
        :param meta:                表元数据
        :return:
        """
        normal_cols = meta[meta.apply(lambda x: x[ADDREQ].not_key() and x[COLUMN] in df.columns, axis=1)][COLUMN]
        inQ2 = [f'`{x}`=values(`{x}`)' for x in normal_cols]

        sql = "insert into `{0}`({1}) values({2}) on duplicate key update {3}".format(
            name,
            ', '.join([f'`{x}`' for x in df.columns]),  # 避免data.columns中有非str类型导致报错
            ', '.join(['%s'] * df.columns.size),
            ', '.join(inQ2))
        vals = _dataframe_2_format_list(df)  # 应用过obj_format处无需再df.replace
        self.execute_many(sql, vals, True)

    def drop_table(self, name: str):
        """
        在Mysql中删除表

        :param name:                表名
        :return:
        """
        sql = f"drop table if exists `{name}`"
        self.execute(sql)

    def select_row_num(self,
                       name: str,
                       span: Optional[DateSpan] = None,
                       groupby: Optional[list[str]] = None) -> Union[int, DataFrame]:
        """
        查询表格的行数

        :param name:            表名
        :param span:            时间范围
        :param groupby:         聚合条件
        :return:                表格行数
        """
        groupby = [] if groupby is None else [ReqCol(x) for x in groupby]
        sql = self._assemble_sql(name, [ReqCol.ROW_NUM()], span, groupby)
        res = self.execute(sql, fetch=True)
        if list_not_valid(groupby):
            return 0 if dataframe_not_valid(res) else res.iloc[0, 0]
        return res

    def select_data(self,
                    name: str,
                    span: Optional[DateSpan] = None) -> Optional[DataFrame]:
        """
        查询表格的数据

        :param name:            表名
        :param span:            时间范围
        :return:                表格行数
        """
        groupby = []
        sql = self._assemble_sql(name, [ReqCol.ALL()], span, groupby)
        res = self.execute(sql, fetch=True)
        return res

    def _assemble_sql(self,
                      name,
                      cols: list[ReqCol],
                      span: DateSpan,
                      groupby: list[ReqCol]):
        """
        组装sql语句for select

        :param name:            表名
        :param cols:            列名
        :param span:            时间区间
        :param groupby:         分组
        :return:
        """
        cols.extend(groupby)
        cols_str = ','.join([x.sql_format() for x in cols])
        where_str = self._span_ext(name, span)
        groupby_str = _groupby_ext(groupby)
        sql = f"select {cols_str} from `{name}` {where_str} {groupby_str}"
        return sql

    def _span_ext(self,
                  name: str,
                  span: Optional[DateSpan]) -> str:
        """
        扩展sql: where

        :param name:            表名
        :param span:            时间区间
        :return:
        """
        if span is None:
            return ''

        sql = f"desc `{name}`"
        res = self.execute(sql, fetch=True)
        key = DATE if DATE in res[FIELD].values else DATETIME
        start_valid = span.start is not None
        end_valid = span.end is not None
        if start_valid and end_valid:
            return f"where `{key}` >= '{span.start}' and `{key}` < '{span.end}'"
        elif start_valid:
            return f"where `{key}` >= '{span.start}'"
        elif end_valid:
            return f"where `{key}` < '{span.end}'"
        return ''
