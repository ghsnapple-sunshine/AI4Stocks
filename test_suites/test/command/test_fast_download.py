from buffett.common.pendulum import DateTime
from buffett.task import (
    StockListTask,
    CalendarTask,
    StockProfitTask,
    ConceptListTask,
    IndustryListTask,
    IndexListTask,
    MoneySupplyTask,
)
from buffett.task.stock_dividend_task import StockDividendTask
from test import Tester


class TestFastDownload(Tester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_download_fast(self):
        """
        download的镜像不循环测试
        （交易日历、股票清单、股票收益、股票除息、概念板块清单、行业板块清单、指数清单、货币供应量）

        :return:
        """
        secs_before = DateTime.now().subtract(seconds=10)
        task_cls = [
            CalendarTask,
            StockListTask,
            StockProfitTask,
            StockDividendTask,
            ConceptListTask,
            IndustryListTask,
            IndexListTask,
            MoneySupplyTask,
        ]
        tasks = [
            task_cls[i](operator=self._operator, start_time=secs_before.add(seconds=i))
            for i in range(0, len(task_cls))
        ]
        for task in tasks:
            task.run()
