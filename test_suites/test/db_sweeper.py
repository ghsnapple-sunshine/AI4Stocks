# 仅用于测试环境
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType


class DbSweeper:
    @staticmethod
    def cleanup():
        op = Operator(RoleType.DbInfo)
        sql = 'SELECT TABLE_NAME from information_schema.tables where table_schema="stockstest"'
        db = op.execute(sql, fetch=True)
        op = Operator(RoleType.DbTest)
        for index, row in db.iterrows():
            table_name = row['TABLE_NAME']
            op.drop_table(name=table_name)
