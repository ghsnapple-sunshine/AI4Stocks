import sys

from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator


class DbSweeper:
    @staticmethod
    def clean_up():
        confirm = input('确认删除stocks中的所有数据？删除后不可恢复。（y/n）')
        if confirm == 'y':
            op = MysqlOperator(MysqlRole.DbInfo)
            sql = 'SELECT TABLE_NAME from information_schema.tables where table_schema="stocks"'
            db = op.execute(sql, fetch=True)
            op = MysqlOperator(MysqlRole.ROOT)
            for index, row in db.iterrows():
                table_name = row['TABLE_NAME']
                op.drop_table(name=table_name)
