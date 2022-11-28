from buffett.adapter.pandas import DataFrame
from buffett.backtrader.frame import Para as WPara, TheWorldBuilder as Builder
from buffett.backtrader.strategy import StrategyBase as Strat
from buffett.common.constants.col import DATE
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.pendulum import Date, DateSpan as Span
from buffett.download import Para as DPara
from buffett.download.handler.calendar import CalendarHandler as CHandler
from buffett.download.handler.list import SseStockListHandler as SHandler
from buffett.download.handler.stock import DcDailyHandler as DHandler
from test import Tester


class WorldTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        # 指定测试周期
        d1, d2, d3 = Date(2022, 1, 4), Date(2022, 1, 5), Date(2022, 1, 6)
        # 初始化交易日历
        cdata = DataFrame({DATE: [d1, d2]})
        CHandler(operator=cls._operator)._save_to_database(cdata)
        # 初始化StockList
        sdata = DataFrame(
            [["000001", "平安银行"], ["600000", "浦发银行"]], columns=[CODE, NAME]
        )
        SHandler(operator=cls._operator)._save_to_database(sdata)
        # 初始化日线数据
        mini_para = DPara().with_start_n_end(start=d1, end=d3)
        DHandler(operator=cls._operator).obtain_data(mini_para)

    def _setup_always(self) -> None:
        pass

    def test_world_flow(self):
        para = WPara(
            operator=self._operator,
            stock_list=["000001", "600000"],
            datespan=Span(start=Date(2022, 1, 4), end=Date(2022, 1, 6)),
            strat_cls=Strat,
        )
        world = (
            Builder().with_clock_n_exchange(para=para).with_strategy(para=para).build()
        )
        world.run()
