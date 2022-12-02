from buffett.common.pendulum import DateTime
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.task import (
    TaskScheduler,
    SseStockListTask,
    DcStockDailyTask,
    StockMinuteTask,
    StockReformTask,
    CalendarTask,
    StockProfitTask,
    StockDividendTask,
    ConceptListTask,
    IndustryListTask,
    IndexListTask,
    MoneySupplyTask,
    StockPePbTask,
    ConceptConsTask,
    ConceptDailyTask,
    IndustryConsTask,
    IndexDailyTask,
    IndustryDailyTask,
    BsStockListTask,
    BsStockDailyTask,
)


def download():
    operator = Operator(RoleType.DbStock)
    now = DateTime.now()
    task_cls = [
        CalendarTask,
        SseStockListTask,
        BsStockListTask,
        StockProfitTask,
        StockDividendTask,
        ConceptListTask,
        IndustryListTask,
        IndexListTask,
        MoneySupplyTask,  # Fast
        StockPePbTask,
        ConceptConsTask,
        ConceptDailyTask,
        IndustryConsTask,
        IndexDailyTask,  # Medium
        DcStockDailyTask,
        BsStockDailyTask,
        StockMinuteTask,
        IndustryDailyTask,
        StockReformTask,  # Slow
    ]
    tasks = [
        task_cls[i](operator=operator, start_time=now.add(seconds=i))
        for i in range(0, len(task_cls))
    ]
    sch = TaskScheduler(operator=operator, tasks=tasks)
    sch.run()
