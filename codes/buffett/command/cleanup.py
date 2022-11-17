from buffett.adapter.pandas import DataFrame
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType


def cleanup() -> None:
    confirm = input("确认删除stocks中的所有数据？删除后不可恢复。（y/n）")
    if confirm == "y":
        db = _scan()
        op = Operator(RoleType.ROOT)
        for index, row in db.iterrows():
            table_name = row["TABLE_NAME"]
            op.drop_table(name=table_name)


def _scan() -> DataFrame:
    op = Operator(RoleType.DbInfo)
    sql = 'SELECT TABLE_NAME from information_schema.tables where table_schema="stocks"'
    db = op.execute(sql, fetch=True)
    return db
