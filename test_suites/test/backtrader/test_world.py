from pandas import DataFrame

from buffett.backtrader.frame import Para as WPara, TheWorldBuilder as Builder
from buffett.backtrader.strategy import StrategyBase as Strat
from buffett.common import Code as Code
from buffett.common.pendelum import Date, DateSpan as Span
from buffett.constants.col import DATE
from buffett.constants.col.stock import CODE, NAME
from buffett.download import Para as HPara
from buffett.download.fast import TradeCalendarHandler as CHandler, StockListHandler as SHandler
from buffett.download.slow import AkDailyHandler as DHandler
from test.common import BaseTest, DbSweeper


class WorldTest(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        # 清理数据库
        DbSweeper.cleanup()
        # 指定测试周期
        start = Date(2022, 1, 4)
        end = Date(2022, 1, 5)
        # 初始化交易日历
        data = [[start.format('YYYY-MM-DD')],
                [end.format('YYYY-MM-DD')]]
        CHandler(operator=self.operator) \
            ._save_to_database(df=DataFrame(data=data, columns=[DATE]))
        # 初始化StockList
        data = [['000001', '平安银行'],
                ['600000', '浦发银行']]
        SHandler(operator=self.operator) \
            ._save_to_database(df=DataFrame(data=data, columns=[CODE, NAME]))
        # 下载日线数据
        DHandler(operator=self.operator) \
            .obtain_data(para=HPara().with_start_n_end(start=start, end=end))

    def test_world_flow(self):
        para = WPara(operator=self.operator,
                     stock_list=[Code('000001'), Code('600000')],
                     datespan=Span(start=Date(2022, 1, 4), end=Date(2022, 1, 5)),
                     strat_cls=Strat)
        world = Builder().with_clock_n_exchange(para=para) \
            .with_strategy(para=para) \
            .build()
        world.run()
