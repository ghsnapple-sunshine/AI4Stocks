from datetime import date, datetime
from enum import Enum

import pandas as pd
from pandas import DataFrame, Timestamp

from ai4stocks.constants.meta import COLUMN, TYPE, ADDREQ
from ai4stocks.download.mysql.connector import Connector


def _obj_format(obj):
    if obj is None:
        return 'NULL'
    # 注意到DateTime类型在DateFrame中被封装为TimeSpan类型
    if isinstance(obj, (str, date, datetime, Timestamp)):
        return '\'{0}\''.format(obj)  # 增加引号
    if isinstance(obj, Enum):
        return obj.value
    return obj


class Operator(Connector):
    def create_table(self,
                     name: str,
                     meta: DataFrame,
                     if_not_exist=True) -> None:
        """
        在Mysql中创建表

        :param name:            名称
        :param meta:            元数据
        :param if_not_exist:    检查表格是否已经存在，如果已经存在则不继续创建
        :return:                None
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
                    data: DataFrame) -> None:
        if (data is None) or (isinstance(data, DataFrame) & data.empty):
            return

        inQ = ['%s'] * data.columns.size
        inQ = ', '.join(inQ)
        cols = data.columns
        cols = ', '.join(cols)
        sql = "insert into `{0}`({1}) values({2})".format(name, cols, inQ)
        vals = data.values.tolist()
        self.execute_many(sql, vals, True)

    def try_insert_data(self,
                        name: str,
                        data: DataFrame,
                        meta: DataFrame = None,
                        update: bool = False):
        if (data is None) or (isinstance(data, DataFrame) & data.empty):
            return

        if update:
            self._try_insert_data_and_update(name=name, data=data, meta=meta)
            return

        sql = "insert ignore into `{0}`({1}) values({2})".format(
            name,
            ', '.join(data.columns),
            ', '.join(['%s'] * data.columns.size)
        )
        vals = data.values.tolist()
        self.execute_many(sql, vals, True)

    def _try_insert_data_and_update(self,
                                    name: str,
                                    data: DataFrame,
                                    meta: DataFrame):
        normal_cols = meta[ADDREQ].apply(lambda x: not x.is_key())
        normal_cols = meta[normal_cols][COLUMN]
        inQ2 = [x + '=%s' for x in normal_cols]

        sql = "insert into `{0}`({1}) values({2}) on duplicate key update {3}".format(
            name,
            ', '.join(data.columns),
            ', '.join(['%s'] * data.columns.size),
            ', '.join(inQ2))
        data = pd.concat([data, data[normal_cols]], axis=1)
        for index, row in data.iterrows():
            ser = row.apply(
                lambda x: _obj_format(x)
            )
            sql2 = sql % tuple(ser)
            self.execute(sql2)
        self.conn.commit()

    def drop_table(self, name: str):
        sql = "drop table if exists `{0}`".format(name)
        self.execute(sql)

    def get_record_cnt(self, name: str):
        sql = "select count(*) from {0}".format(name)
        res = self.execute(sql, fetch=True)
        return res.iloc[0, 0]

    def get_table(self, name: str):
        sql = "select * from {0}".format(name)
        res = self.execute(sql, fetch=True)
        return res
