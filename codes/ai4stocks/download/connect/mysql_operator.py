from enum import Enum
from pendulum import DateTime

from ai4stocks.common.constants import COL_META_COLUMN, COL_META_TYPE, COL_META_ADDREQ
from ai4stocks.common.stock_code import StockCode
from ai4stocks.download.connect.mysql_connector import MysqlConnector
from pandas import DataFrame, Timestamp
import pandas as pd


class MysqlOperator(MysqlConnector):
    def CreateTable(self, name: str, col_meta: DataFrame, if_not_exist=True):
        cols = []
        for index, row in col_meta.iterrows():
            s = row[COL_META_COLUMN] + ' ' + row[COL_META_TYPE].ToSql() + ' ' + row[COL_META_ADDREQ].ToSql()
            cols.append(s)

        is_key = col_meta[COL_META_ADDREQ].apply(lambda x: x.IsKey())
        prim_key = col_meta[is_key][COL_META_COLUMN]
        if not prim_key.empty:
            apd = ','.join(prim_key)
            apd = 'primary key ({0})'.format(apd)
            cols.append(apd)

        joinCols = ','.join(cols)
        str_if_exist = 'if not exists ' if if_not_exist else ''
        sql = 'create table {0}`{1}` ({2})'.format(str_if_exist, name, joinCols)
        self.Execute(sql)

    def InsertData(self, name: str, df: DataFrame):
        if df is None or isinstance(df, DataFrame) and df.shape[0] <= 0:
            return

        inQ = ['%s'] * df.columns.size
        inQ = ', '.join(inQ)
        cols = df.columns
        cols = ', '.join(cols)
        sql = "insert into `{0}`({1}) values({2})".format(name, cols, inQ)
        vals = df.values.tolist()
        self.ExecuteMany(sql, vals, True)

    def TryInsertData(self, name: str, df: DataFrame, col_meta=None, update=False):
        if df is None or isinstance(df, DataFrame) and df.shape[0] <= 0:
            return

        inQ = ['%s'] * df.columns.size
        strInQ = ', '.join(inQ)
        cols = df.columns
        strCols = ', '.join(cols)

        if update:
            self.TryInsertDataAndUpdate(name=name, df=df, col_meta=col_meta, strCols=strCols, strInQ=strInQ)
            return

        sql = "insert ignore into `{0}`({1}) values({2})".format(name, strCols, strInQ)
        vals = df.values.tolist()
        self.ExecuteMany(sql, vals, True)

    def TryInsertDataAndUpdate(self, name: str, df: DataFrame, col_meta: DataFrame, strCols: str, strInQ: str):
        normal_cols = col_meta[COL_META_ADDREQ].apply(lambda x: not x.IsKey())
        normal_cols = col_meta[normal_cols][COL_META_COLUMN]
        inQ2 = [x + '=%s' for x in normal_cols]
        strInQ2 = ', '.join(inQ2)
        sql = "insert into `{0}`({1}) values({2}) on duplicate key update {3}".format(
            name, strCols, strInQ, strInQ2)
        df = pd.concat([df, df[normal_cols]], axis=1)
        for row in range(df.shape[0]):
            ls = list(df.iloc[row, :])
            ls = [MysqlOperator.ObjFormat(obj) for obj in ls]
            sql2 = sql % tuple(ls)
            self.Execute(sql2)
        self.conn.commit()

    @staticmethod
    def ObjFormat(obj):
        # 注意到DateTime类型在DateFrame中被封装为TimeSpan类型
        if isinstance(obj, (str, DateTime, Timestamp, StockCode)):
            return '\'{0}\''.format(obj)  # 增加引号
        if isinstance(obj, Enum):
            return obj.value
        return obj

    def DropTable(self, name: str):
        sql = "drop table if exists `{0}`".format(name)
        self.Execute(sql)

    def GetTableCnt(self, name: str):
        sql = "select count(*) from {0}".format(name)
        res = self.Execute(sql, fetch=True)
        return res.iloc[0, 0]

    def GetTable(self, name: str):
        sql = "select * from {0}".format(name)
        res = self.Execute(sql, fetch=True)
        return res
