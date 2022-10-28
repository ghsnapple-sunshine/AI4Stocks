from pandas import DataFrame
from pendulum import DateTime, Duration

from ai4stocks.download.mysql import RoleType, Operator
from ai4stocks.task import StockDailyTask, StockListTask, StockMinuteTask, TaskScheduler


def download():
    now = DateTime.now()
    sch = TaskScheduler(
        op=Operator(RoleType.DbStock),
        tasks=[
            StockListTask(plan_time=now),
            StockDailyTask(plan_time=now + Duration(seconds=1)),
            StockMinuteTask(plan_time=now + Duration(seconds=2))]
    )
    sch.run()


def cleanup() -> None:
    confirm = input('确认删除stocks中的所有数据？删除后不可恢复。（y/n）')
    if confirm == 'y':
        db = __scan__()
        op = Operator(RoleType.ROOT)
        for index, row in db.iterrows():
            table_name = row['TABLE_NAME']
            op.drop_table(name=table_name)


def __scan__() -> DataFrame:
    op = Operator(RoleType.DbInfo)
    sql = 'SELECT TABLE_NAME from information_schema.tables where table_schema="stocks"'
    db = op.execute(sql, fetch=True)
    return db
