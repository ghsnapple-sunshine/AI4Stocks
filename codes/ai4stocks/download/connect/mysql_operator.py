from pendulum import DateTime

from ai4stocks.download.connect.mysql_connector import MysqlConnector
from ai4stocks.download.connect.mysql_common import MysqlConstants
from pandas import DataFrame, Timestamp
import pandas as pd


class MysqlOperator(MysqlConnector):
    def CreateTable(self, name: str, colDesc: DataFrame, if_not_exist=True):
        strColumn = MysqlConstants.META_COLS[0]
        strType = MysqlConstants.META_COLS[1]
        strAddReq = MysqlConstants.META_COLS[2]
        cols = []
        for index, row in colDesc.iterrows():
            s = row[strColumn] + ' ' + row[strType].ToSql() + ' ' + row[strAddReq].ToSql()
            cols.append(s)
        joinCols = ','.join(cols)
        strIfExist = 'if not exists ' if if_not_exist else ''
        sql = 'create table {0}`{1}` ({2})'.format(strIfExist, name, joinCols)
        self.Execute(sql)

    def InsertData(self, name: str, df: DataFrame):
        inQ = ['%s'] * df.columns.size
        inQ = ', '.join(inQ)
        cols = df.columns
        cols = ', '.join(cols)
        sql = "insert into `{0}`({1}) values({2})".format(name, cols, inQ)
        vals = df.values.tolist()
        self.ExecuteMany(sql, vals, True)

    def TryInsertData(self, name: str, df: DataFrame, update=False):
        if df is None or isinstance(df, DataFrame) and df.shape[0] <= 0:
            return

        inQ = ['%s'] * df.columns.size
        strInQ = ', '.join(inQ)
        cols = df.columns
        colNum = cols.size
        strCols = ', '.join(cols)
        if not update:
            sql = "insert ignore into `{0}`({1}) values({2})".format(name, strCols, strInQ)
            vals = df.values.tolist()
            self.ExecuteMany(sql, vals, True)
        else:
            inQ2 = df.columns[1:] + "=%s"
            strInQ2 = ', '.join(inQ2)
            sql = "insert into `{0}`({1}) values({2}) on duplicate key update {3}".format(
                name, strCols, strInQ, strInQ2)
            df = pd.concat([df, df.iloc[:, 1:colNum]], axis=1)
            for row in range(df.shape[0]):
                ls = list(df.iloc[row, :])
                ls = ['\'{0}\''.format(obj) if MysqlOperator.IsAddComma(obj) else obj
                      for obj in ls]
                sql2 = sql % tuple(ls)
                self.Execute(sql2)
            self.conn.commit()

    @staticmethod
    def IsAddComma(obj):
        if isinstance(obj, str):
            return True
        elif isinstance(obj, DateTime) | isinstance(obj, Timestamp):  # 注意到DateTime类型在DateFrame中被封装为TimeSpan类型
            return True
        else:
            return False

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
