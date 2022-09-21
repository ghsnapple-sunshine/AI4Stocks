from codes.data_connect.mysql_connector import MysqlConnector
from codes.data_connect.mysql_common import MysqlConstants, MysqlColumnType
from pandas import DataFrame
import pandas as pd


class MysqlOperator(MysqlConnector):
    def CreateTable(self, table_name: str, colDesc: DataFrame, ifNotExt=True):
        strColumn = MysqlConstants.COLUMN_INDEXS[0]
        strType = MysqlConstants.COLUMN_INDEXS[1]
        dictType = MysqlConstants.COLUMN_TYPE_DICT
        strAddReq = MysqlConstants.COLUMN_INDEXS[2]
        dictAddReq = MysqlConstants.COLUMNS_ADDREQ_DICT
        cols = []
        for index, row in colDesc.iterrows():
            s = row[strColumn] + ' ' + dictType[row[strType]] + ' ' + dictAddReq[row[strAddReq]]
            cols.append(s)
        joinCols = ','.join(cols)
        strIfExist = 'if not exists ' if ifNotExt else ''
        sql = 'create table {0}`{1}` ({2})'.format(strIfExist, table_name, joinCols)
        self.execute(sql)

    def InsertData(self, tableName: str, df: DataFrame):
        inQ = ['%s'] * df.columns.size
        inQ = ', '.join(inQ)
        cols = df.columns
        cols = ', '.join(cols)
        sql = "insert into `{0}`({1}) values({2})".format(tableName, cols, inQ)
        vals = df.values.tolist()
        self.executeMany(sql, vals, True)

    def TryInsertData(self, tableName: str, df: DataFrame, update=False):
        inQ = ['%s'] * df.columns.size
        strInQ = ', '.join(inQ)
        cols = df.columns
        colNum = cols.size
        strCols = ', '.join(cols)
        if not update:
            sql = "insert ignore into `{0}`({1}) values({2})".format(tableName, strCols, strInQ)
            vals = df.values.tolist()
            self.executeMany(sql, vals, True)
        else:
            inQ2 = df.columns[1:] + "=%s"
            strInQ2 = ', '.join(inQ2)
            sql = "insert into `{0}`({1}) values({2}) on duplicate key update {3}".format(tableName, strCols, strInQ,
                                                                                          strInQ2)
            df = pd.concat([df, df.iloc[:, 1:colNum]], axis=1)
            for row in range(df.shape[0]):
                ls = list(df.iloc[row, :])
                ls = ['\'{0}\''.format(obj) if isinstance(obj, str) else obj
                      for obj in ls]
                sql = sql % tuple(ls)
                self.execute(sql)
            self.conn.commit()

    '''
    def TryInsertData(self, tableName: str, df: DataFrame):
        engine = create_engine()
        pd.io.sql.to_sql(df, tableName, )
    '''

    def DropTable(self, tableName: str):
        sql = "drop table if exists `{0}`".format(tableName)
        self.execute(sql)

    def GetTableCnt(self, tableName: str):
        sql = "select count(*) from {0}".format(tableName)
        res = self.execute(sql, fetch=True)
        return res[0]

    def GetTable(self, tableName: str):
        sql = "select * from {0}".format(tableName)
        res = self.execute(sql, fetch=True)
        return res
