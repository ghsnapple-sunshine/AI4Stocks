from typing import Type

from buffett.adapter.pandas import DataFrame
from buffett.backtrader.frame.account import Account as Acc, AccountBuilder as AccBuilder
from buffett.backtrader.frame.accounting_manager import AccountingManager as AccMan, \
    AccountManagerBuilder as AMBuilder
from buffett.backtrader.frame.clock import Clock
from buffett.backtrader.frame.clock_manager import ClockManager as ClockMan, ClockManagerBuilder as CMBuilder
from buffett.backtrader.frame.order_manager import OrderManager as OrderMan, OrderManagerBuilder as OMBuilder
from buffett.backtrader.frame.stock_info_manager import StockInfoManager as StockMan, \
    StockInfoManagerBuilder as SMBuilder
from buffett.backtrader.interface.time_sequence import ITimeSequence as Sequence
from buffett.backtrader.tools import ChargeCalculator as Calc
from buffett.common import Code as Code
from buffett.common.pendelum import DateSpan as Span
from buffett.common.wrapper import Wrapper
from buffett.download import Handler as Handler
from buffett.download.mysql import Operator as Operator
from buffett.download.types import HeadType


class Exchange(Sequence):
    def __init__(self):
        super().__init__()
        self._clock_man: ClockMan = ClockMan()
        self._stock_man: StockMan = StockMan()
        self._order_man: OrderMan = OrderMan()
        self._accounting_man: AccMan = AccMan()
        self._account: Acc = Acc()

    def run(self):
        # self._stock_man.run()     # 1. 更新StockInfo(分段载入）
        self._order_man.run()       # 2. 分发（和处理）委托单
        self._account.run()         # 3. 时刻末计算
        self._clock_man.run()       # 4. 时刻前进

    def get_holding(self, code: int) -> int:
        return self._account.get_holding(code)


class ExchangeBuilder:
    class ExchangeUnderBuilt(Exchange):
        def __init__(self):
            super().__init__()
            self.stock_man_builder = SMBuilder()
            self.order_man_builder = OMBuilder()
            self.accounting_man_builder = AMBuilder()
            self.account_builder = AccBuilder()

        def set_clock(self, clock: Clock):
            self._clock = clock

        def set_clock_man(self, clock_man: ClockMan):
            self._clock_man = clock_man

        def set_stock_man(self, stock_man: StockMan):
            self._stock_man = stock_man

        def set_order_man(self, order_man: OrderMan):
            self._order_man = order_man

        def set_accounting_man(self, accounting_man: AccMan):
            self._accounting_man = accounting_man

        def set_account(self, account: Acc):
            self._account = account

        def get_tick_num(self) -> int:
            return self._clock_man.max_tick

        def get_calendar(self) -> DataFrame:
            return self._clock_man.calendar

        def get_clock(self) -> Clock:
            return self._clock

        def get_clock_man(self):
            return self._clock_man

    def __init__(self):
        self.item = ExchangeBuilder.ExchangeUnderBuilt()

    def with_clock_man(
            self,
            operator: Operator,
            datespan: Span,
            clock: Clock):
        self.item.set_clock(clock=clock)
        clock_man = CMBuilder().with_calendar(
            operator=operator,
            datespan=datespan
        ).with_clock(
            clock=clock
        ).build()
        self.item.set_clock_man(clock_man=clock_man)
        return self

    def with_stock_man(
            self,
            operator: Operator,
            stock_list: list[Code],
            datespan: Span,
            add_cols: dict[HeadType, Type[Handler]]
    ):
        """
        初始化StockInfoManager（依赖于ClockManager）

        :param operator:
        :param stock_list:
        :param datespan:
        :param add_cols:
        :return:                self
        """
        calendar = self.item.get_calendar()
        stock_man_builder = SMBuilder().with_stock_list(
            stock_list=stock_list
        ).with_infos(
            operator=operator,
            add_cols=add_cols,
            calendar=calendar,
            datespan=datespan,
            clock=self.item.get_clock())
        self.item.stock_man_builder = stock_man_builder
        return self

    def with_order_man(self):
        """
        初始化OrderManager（依赖于ClockManager）

        :return:                self
        """
        order_man = OMBuilder().with_clock(
            clock=self.item.get_clock()
        ).build()
        self.item.set_order_man(order_man=order_man)
        return self

    def with_accounting_man(self, charge_calc: Calc):
        """
        初始化AccountingManager（依赖于ClockManager）

        :param charge_calc:
        :return:                self
        """
        accounting_man_builder = AMBuilder() \
            .with_charge_calc(charge_calc=charge_calc) \
            .with_clock(clock=self.item.get_clock())
        self.item.accounting_man_builder = accounting_man_builder
        return self

    def with_account(self, stock_num: int, init_cash: int):
        """
        初始化Account（依赖于ClockManager)

        :param stock_num:
        :param init_cash:
        :return:
        """
        max_tick = self.item.get_tick_num()
        account_builder = AccBuilder().with_holdings_and_cash(
            stock_num=stock_num,
            max_tick=max_tick,
            init_cash=init_cash
        ).with_clock(
            clock=self.item.get_clock())
        self.item.account_builder = account_builder
        return self

    def build(self):
        # 将AccountManager的handle_order方法传递给OrderManager
        self.item.order_man_builder.with_handle_order(
            handle_order=Wrapper(self.item.accounting_man_builder.item.handle_order))
        # 将StockInfoManager的get_all_stock_close方法传递给Account
        self.item.account_builder.with_all_close(
            get_all_close=Wrapper(self.item.stock_man_builder.item.get_all_close))
        # 将StockInfoManager的get_curr_olhc方法传递给AccountingManager
        self.item.accounting_man_builder.with_olhc(
            get_curr_olhc=Wrapper(self.item.stock_man_builder.item.get_curr_olhc))
        # 将Account的get_holding方法传递给AccountingManager
        self.item.accounting_man_builder.with_holding(
            get_holding=Wrapper(self.item.account_builder.item.get_holding))
        # build剩余四个对象
        self.item.set_stock_man(
            stock_man=self.item.stock_man_builder.build())
        self.item.set_order_man(
            order_man=self.item.order_man_builder.build())
        self.item.set_accounting_man(
            accounting_man=self.item.accounting_man_builder.build())
        self.item.set_account(
            account=self.item.account_builder.build())
        self.item.get_clock_man().run()  # ClockManager把时间设置为开始
        self.item.__class__ = Exchange
        return self.item
