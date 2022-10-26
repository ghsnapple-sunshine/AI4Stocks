from pandas import DataFrame

from ai4stocks.backtrader.frame import TheWorldParameter as WPara, TheWorldBuilder as Builder
from ai4stocks.backtrader.strategy import StrategyBase as Strat
from ai4stocks.common import StockCode as Code, COL_STOCK_CODE as CODE, COL_STOCK_NAME as NAME, COL_DATE as DATE
from ai4stocks.common.pendelum import Date, DateSpan as Span
from ai4stocks.download.fast import TradeCalendarHandler as CHandler, StockListHandler as SHandler
from ai4stocks.download.slow import AkStockDailyHandler as DHandler
from ai4stocks.download import HandlerParameter as HPara
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
        CHandler(operator=self.op) \
            .__save_to_database__(df=DataFrame(data=data, columns=[DATE]))
        # 初始化StockList
        data = [['000001', '平安银行'],
                ['600000', '浦发银行']]
        SHandler(operator=self.op) \
            .__save_to_database__(df=DataFrame(data=data, columns=[CODE, NAME]))
        # 下载日线数据
        DHandler(operator=self.op) \
            .obtain(para=HPara(datespan=Span(start=start, end=end)))

    def test_world_flow(self):
        para = WPara(
            operator=self.op,
            stock_list=[Code('000001'), Code('600000')],
            datespan=Span(start=Date(2022, 1, 4), end=Date(2022, 1, 5)),
            strat_cls=Strat)
        world = Builder().with_clock_n_exchange(para=para) \
            .with_strategy(para=para) \
            .build()
        world.run()
