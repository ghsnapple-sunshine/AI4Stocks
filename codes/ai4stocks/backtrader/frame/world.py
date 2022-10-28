from __future__ import annotations

from ai4stocks.backtrader.frame.clock_manager import Clock
from ai4stocks.backtrader.frame.exchange import ExchangeBuilder as EBuilder, Exchange as Exchange
from ai4stocks.backtrader.interface import ITimeSequence as Sequence
from ai4stocks.backtrader.strategy import StrategyBase as Strat, StrategyBuilder as SBuilder
from ai4stocks.backtrader.tools import ChargeCalculator as Calc
from ai4stocks.common import Code as Code
from ai4stocks.common.pendelum import DateSpan as Span
from ai4stocks.download.mysql import Operator as Operator


class Para:
    def __init__(
            self,
            operator: Operator,
            stock_list: list[Code],
            datespan: Span,
            strat_cls: type[Strat],
            init_cash: int = 1000000,
            charge_calculator: Calc = Calc()
    ):
        """
        初始化仿真参数

        :param operator:            Mysql操作器
        :param stock_list:          股票列表
        :param datespan:            模拟的时间周期
        :param strat_cls:           策略builder
        :param init_cash:           初始资金（元）
        :param charge_calculator:   税费计算器
        """
        self.operator = operator
        self.stock_list = stock_list
        self.datespan = datespan
        self.charge_calc = charge_calculator
        self.strat_cls = strat_cls
        self.init_cash = init_cash


class TheWorld(Sequence):
    def __init__(self):
        super().__init__()
        self._strat: Strat = Strat()
        self._exchange: Exchange = Exchange()

    def run(self):
        while not self.is_end:
            self._strat.run()  # 下单
            self._exchange.run()  # 处理订单


class TheWorldBuilder:
    class TheWorldUnderBuilt(TheWorld):
        def set_strat(self, strat: Strat) -> None:
            self._strat = strat

        def set_clock(self, clock: Clock) -> None:
            self._clock = clock

        def set_exchange(self, exchange: Exchange) -> None:
            self._exchange = exchange

        def get_exchange(self) -> Exchange:
            return self._exchange

    def __init__(self):
        self.item = TheWorldBuilder.TheWorldUnderBuilt()

    def with_clock_n_exchange(self, para: Para) -> TheWorldBuilder:
        """
        设置Clock和Exchange（调用次序：1）

        :param para:
        :return:
        """
        clock = Clock()
        self.item.set_clock(clock=clock)

        exchange = EBuilder().with_clock_man(
            operator=para.operator,
            datespan=para.datespan,
            clock=clock
        ).with_account(
            stock_num=len(para.stock_list),
            init_cash=para.init_cash).with_accounting_man(
            charge_calc=para.charge_calc
        ).with_order_man().with_stock_man(
            operator=para.operator,
            stock_list=para.stock_list,
            datespan=para.datespan,
            add_cols=para.strat_cls.col_request()
        ).build()

        self.item.set_exchange(exchange=exchange)
        return self

    def with_strategy(self, para: Para) -> TheWorldBuilder:
        """
        设置策略（调用次序2）

        :param para:
        :return:
        """
        strat = SBuilder(origin_cls=para.strat_cls) \
            .with_exchange(exchange=self.item.get_exchange()) \
            .build()
        self.item.set_strat(strat=strat)
        return self

    def build(self) -> TheWorld:
        self.item.__class__ = TheWorld
        return self.item

