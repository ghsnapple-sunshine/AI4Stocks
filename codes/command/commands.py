from pandas import DataFrame

from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.task.stock_minute_task import StockMinuteTask
from ai4stocks.task.task_scheduler import TaskScheduler


def download():
    sch = TaskScheduler(
        op=MysqlOperator(MysqlRole.DbStock),
        # tasks=[StockListTask(), StockDailyTask(), StockMinuteTask()]
        tasks=[StockMinuteTask()]
    )
    sch.run()


def cleanup() -> None:
    confirm = input('确认删除stocks中的所有数据？删除后不可恢复。（y/n）')
    if confirm == 'y':
        db = scan()
        op = MysqlOperator(MysqlRole.ROOT)
        for index, row in db.iterrows():
            table_name = row['TABLE_NAME']
            op.drop_table(name=table_name)


def scan() -> DataFrame:
    op = MysqlOperator(MysqlRole.DbInfo)
    sql = 'SELECT TABLE_NAME from information_schema.tables where table_schema="stocks"'
    db = op.execute(sql, fetch=True)
    return db

