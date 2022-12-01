from buffett.common.pendulum import DateTime
from buffett.task import (
    SseStockListTask,
    CalendarTask,
    StockProfitTask,
    ConceptListTask,
    IndustryListTask,
    IndexListTask,
    MoneySupplyTask,
)
from buffett.task.stock_dividend_task import StockDividendTask
from test import Tester
from test.command.tools import create_task_no_subsequent


class TestFastDownload(Tester):
    @classmethod
    def _setup_oncemore(cls):
        # 创建任务清单
        cls._task_cls = [
            create_task_no_subsequent(CalendarTask),
            create_task_no_subsequent(SseStockListTask),
            create_task_no_subsequent(StockProfitTask),
            create_task_no_subsequent(StockDividendTask),
            create_task_no_subsequent(ConceptListTask),
            create_task_no_subsequent(IndustryListTask),
            create_task_no_subsequent(IndexListTask),
            create_task_no_subsequent(MoneySupplyTask),
        ]

    def _setup_always(self) -> None:
        pass

    def test_download_fast(self):
        """
        download的镜像不循环测试
        （交易日历、股票清单、股票收益、股票除息、概念板块清单、行业板块清单、指数清单、货币供应量）

        :return:
        """
        task_num = len(self._task_cls)
        operator = self._operator
        secs_before = DateTime.now().subtract(seconds=task_num)
        tasks = [
            self._task_cls[i](operator=operator, start_time=secs_before.add(seconds=i))
            for i in range(0, task_num)
        ]
        for t in tasks:
            t.run()
