from pandas import DataFrame

from ai4stocks.download.connect import MysqlOperator, MysqlRole


def scan() -> DataFrame:
    op = MysqlOperator(MysqlRole.DbInfo)
    sql = 'SELECT TABLE_NAME from information_schema.tables where table_schema="stocks"'
    db = op.execute(sql, fetch=True)
    return db
