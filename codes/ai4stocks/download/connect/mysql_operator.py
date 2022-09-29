from enum import Enum
from pendulum import DateTime

from ai4stocks.common.constants import COL_META_COLUMN, COL_META_TYPE, COL_META_ADDREQ
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.connect.mysql_connector import MysqlConnector
from pandas import DataFrame, Timestamp
import pandas as pd


def __obj_format__(obj):
    # 注意到DateTime类型在DateFrame中被封装为TimeSpan类型
    if isinstance(obj, (str, DateTime, Timestamp, StockCode)):
        return '\'{0}\''.format(obj)  # 增加引号
    if isinstance(obj, Enum):
        return obj.value
    return obj


class MysqlOperator(MysqlConnector):
    def create_table(
            self,
            name: str,
            col_meta: DataFrame,
            if_not_exist=True
    ):
        cols = []
        for index, row in col_meta.iterrows():
            s = row[COL_META_COLUMN] + ' ' + row[COL_META_TYPE].to_sql() + ' ' + row[COL_META_ADDREQ].to_sql()
            cols.append(s)

        is_key = col_meta[COL_META_ADDREQ].apply(lambda x: x.is_key())
        prim_key = col_meta[is_key][COL_META_COLUMN]

        if not prim_key.empty:
            apd = ','.join(prim_key)
            apd = 'primary key ({0})'.format(apd)
            cols.append(apd)

        joinCols = ','.join(cols)
        str_if_exist = 'if not exists ' if if_not_exist else ''
        sql = 'create table {0}`{1}` ({2})'.format(str_if_exist, name, joinCols)
        self.execute(sql)

    def insert_data(
            self,
            name: str,
            data: DataFrame
    ):
        if (data is None) or (isinstance(data, DataFrame) & data.empty):
            return

        inQ = ['%s'] * data.columns.size
        inQ = ', '.join(inQ)
        cols = data.columns
        cols = ', '.join(cols)
        sql = "insert into `{0}`({1}) values({2})".format(name, cols, inQ)
        vals = data.values.tolist()
        self.execute_many(sql, vals, True)

    def try_insert_data(
            self,
            name: str,
            data: DataFrame,
            col_meta=None,
            update=False
    ):
        if (data is None) or (isinstance(data, DataFrame) & data.empty):
            return

        inQ = ['%s'] * data.columns.size
        strInQ = ', '.join(inQ)
        cols = data.columns
        strCols = ', '.join(cols)

        if update:
            self.__try_insert_data_and_update__(
                name=name,
                data=data,
                col_meta=col_meta,
                strCols=strCols,
                strInQ=strInQ)
            return

        sql = "insert ignore into `{0}`({1}) values({2})".format(name, strCols, strInQ)
        vals = data.values.tolist()
        self.execute_many(sql, vals, True)

    def __try_insert_data_and_update__(
            self,
            name: str,
            data: DataFrame,
            col_meta: DataFrame,
            strCols: str,
            strInQ: str
    ):
        normal_cols = col_meta[COL_META_ADDREQ].apply(lambda x: not x.is_key())
        normal_cols = col_meta[normal_cols][COL_META_COLUMN]
        inQ2 = [x + '=%s' for x in normal_cols]
        strInQ2 = ', '.join(inQ2)
        sql = "insert into `{0}`({1}) values({2}) on duplicate key update {3}".format(
            name, strCols, strInQ, strInQ2)
        data = pd.concat([data, data[normal_cols]], axis=1)
        for row in range(data.shape[0]):
            ls = list(data.iloc[row, :])
            ls = [__obj_format__(obj) for obj in ls]
            sql2 = sql % tuple(ls)
            self.execute(sql2)
        self.conn.commit()

    def drop_table(self, name: str):
        sql = "drop table if exists `{0}`".format(name)
        self.execute(sql)

    def get_table_cnt(self, name: str):
        sql = "select count(*) from {0}".format(name)
        res = self.execute(sql, fetch=True)
        return res.iloc[0, 0]

    def get_table(self, name: str):
        sql = "select * from {0}".format(name)
        res = self.execute(sql, fetch=True)
        return res
