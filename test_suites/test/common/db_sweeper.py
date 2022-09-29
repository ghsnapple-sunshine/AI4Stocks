# 仅用于测试环境
from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator


class DbSweeper:
    @staticmethod
    def clean_up():
        op = MysqlOperator(MysqlRole.DbInfo)
        sql = 'SELECT TABLE_NAME from information_schema.tables where table_schema="stockstest"'
        db = op.execute(sql, fetch=True)
        op = MysqlOperator(MysqlRole.DbTest)
        for index, row in db.iterrows():
            table_name = row['TABLE_NAME']
            op.drop_table(name=table_name)
