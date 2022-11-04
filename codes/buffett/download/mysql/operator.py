from enum import Enum
from typing import Optional

import numpy as np
import pandas as pd
from pandas import DataFrame

from buffett.common.pendelum import DateSpan
from buffett.common.tools import dataframe_not_valid
from buffett.constants.col import DATE, DATETIME
from buffett.constants.meta import COLUMN, TYPE, ADDREQ
from buffett.download.mysql.connector import Connector


def _obj_format(obj):
    """
    将对象转换成可被sql插入的格式

    :param obj:             待插入的对象
    :return:
    """
    if pd.isna(obj):
        return 'NULL'
    if isinstance(obj, Enum):
        return str(obj.value)
    # 注意到DateTime类型在DateFrame中被封装为TimeSpan类型
    return f'\'{obj}\''  # 增加引号


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
        cols = []
        for index, row in meta.iterrows():
            s = '{0} {1} {2}'.format(row[COLUMN], row[TYPE].sql_format(), row[ADDREQ].sql_format())
            cols.append(s)

        is_key = meta[ADDREQ].apply(lambda x: x.is_key())
        prim_key = meta[is_key][COLUMN]

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
            ', '.join(df.columns),
            ', '.join(['%s'] * df.columns.size))
        df.replace(np.NAN, None, inplace=True)
        vals = df.values.tolist()
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
            self._try_insert_n_update_data(name=name, df=df, meta=meta)
            return

        sql = "insert ignore into `{0}`({1}) values({2})".format(
            name,
            ', '.join(df.columns),
            ', '.join(['%s'] * df.columns.size))
        df.replace(np.NAN, None, inplace=True)
        vals = df.values.tolist()
        self.execute_many(sql, vals, True)

    def _try_insert_n_update_data(self,
                                  name: str,
                                  df: DataFrame,
                                  meta: DataFrame) -> None:
        """
        尝试插入数据到Mysql表（插入失败则更新）

        :param name:                表名
        :param df:                  数据
        :param meta:                表元数据
        :return:
        """
        normal_cols = meta[ADDREQ].apply(lambda x: x.not_key())
        normal_cols = meta[normal_cols][COLUMN]
        inQ2 = [x + '=%s' for x in normal_cols]

        sql = "insert into `{0}`({1}) values({2}) on duplicate key update {3}".format(
            name,
            ', '.join([str(x) for x in df.columns]),  # 避免data.columns中有非str类型导致报错
            ', '.join(['%s'] * df.columns.size),
            ', '.join(inQ2))
        df = pd.concat([df, df[normal_cols]], axis=1)
        for index, row in df.iterrows():
            ser = row.apply(lambda x: _obj_format(x))
            sql2 = sql % tuple(ser)
            self.execute(sql2)
        self.conn.commit()

    def drop_table(self, name: str):
        """
        在Mysql中删除表

        :param name:                表名
        :return:
        """
        sql = f"drop table if exists `{name}`"
        self.execute(sql)

    def get_row_num(self, name: str) -> int:
        """
        获取表格的行数

        :param name:            表名
        :return:                表格行数
        """
        sql = f"select count(*) from `{name}`"
        res = self.execute(sql, fetch=True)
        return 0 if dataframe_not_valid(res) else res.iloc[0, 0]

    def get_data(self,
                 name: str,
                 span: DateSpan = None) -> Optional[DataFrame]:
        """
        按照条件查询表格数据

        :param name:            表名
        :param span:            时间条件
        :return:
        """
        if span is None:
            sql = f"select * from `{name}`"
            res = self.execute(sql, fetch=True)
            return res
        elif isinstance(span, DateSpan):
            sql = f"select * from `{name}` limit 0,1"
            res = self.execute(sql, fetch=True)
            if dataframe_not_valid(res):
                return res

            key = DATE if any([x == DATE for x in res.columns]) else DATETIME
            sql = f"select * from `{name}` where `{key}` >= '{span.start.format('YYYY-MM-DD')}' " \
                  f"and `{key}` < '{span.end.format('YYYY-MM-DD')}'"
            res = self.execute(sql, fetch=True)
            return res
        else:
            return
