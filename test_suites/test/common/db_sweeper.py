# 仅用于测试环境
from ai4stocks.data_connect.mysql_common import MysqlRole
from ai4stocks.data_connect.mysql_operator import MysqlOperator


class DbSweeper:
    @staticmethod
    def CleanUp():
        op = MysqlOperator(MysqlRole.DbInfo)
        sql = 'SELECT TABLE_NAME from information_schema.tables where table_schema="stockstest"'
        db = op.Execute(sql, fetch=True)
        op = MysqlOperator(MysqlRole.DbTest)
        for index, row in db.iterrows():
            table_name = row['TABLE_NAME']
            op.DropTable(name=table_name)