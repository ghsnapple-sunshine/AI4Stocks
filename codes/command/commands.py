from pendulum import DateTime, Duration

from ai4stocks.download.connect import MysqlRole, MysqlOperator
from ai4stocks.task import StockDailyTask, StockListTask, StockMinuteTask, TaskScheduler
from command._commands import scan


def download():
    now = DateTime.now()
    sch = TaskScheduler(
        op=MysqlOperator(MysqlRole.DbStock),
        tasks=[
            StockListTask(plan_time=now),
            StockDailyTask(plan_time=now + Duration(seconds=1)),
            StockMinuteTask(plan_time=now + Duration(seconds=2))]
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



