import numpy as np
from numpy import ndarray

from ai4stocks.backtrader.frame.clock import Clock
from ai4stocks.backtrader.frame.order import Order
from ai4stocks.backtrader.interface import ITimeSequence as Sequence
from ai4stocks.common.wrapper import Wrapper


class Account(Sequence):
    def __init__(self):
        """
        初始化账户
        """
        super().__init__()
        self._holdings: ndarray = ndarray(shape=(1,))
        self._init_cash: float = 0
        self._cash: ndarray = ndarray(shape=(1,))
        self._stock_value: ndarray = ndarray(shape=(1,))
        self._get_all_close: Wrapper = Wrapper()            # 获取当前时刻所有股票的收盘价

    def get_holding(self, code: int) -> int:
        return self._holdings[self.curr_tick, code]

    def get_cash(self) -> int:
        return self._cash[self.curr_tick]

    def trade_success(self, order: Order) -> None:
        """
        交易成功后，刷新持仓和持币
        :param order:   订单
        :return:
        """
        self._holdings[self.curr_tick, order.code] += order.order_num
        self._cash -= order.order_num * order.deal_price

    def run(self) -> None:
        """
        本个时刻末，刷新持仓价值；并滚动到下一个时刻
        :return:
        """
        close = self._get_all_close.run()
        self._stock_value[self.curr_tick] = np.sum(np.multiply(close, self._cash[self.curr_tick]))
        if self._cash[self.curr_tick] < 0:
            self._cash[self.curr_tick] *= 2  # 持币为负时，处以惩罚
        self._holdings[self.curr_tick + 1] = self._holdings[self.curr_tick]
        self._cash[self.curr_tick + 1] = self._cash[self.curr_tick]


class AccountBuilder:
    class AccountUnderBuilt(Account):
        def set_holdings(self, holdings: ndarray):
            self._holdings = holdings

        def set_cash(self, cash: ndarray):
            self._cash = cash

        def set_init_cash(self, init_cash: float):
            self._init_cash = init_cash

        def set_stock_value(self, stock_value: ndarray):
            self._stock_value = stock_value

        def set_all_close(self, get_all_close: Wrapper):
            self._get_all_close = get_all_close

        def set_clock(self, clock: Clock):
            self._clock = clock

    def __init__(self):
        self.item = AccountBuilder.AccountUnderBuilt()

    def with_holdings_and_cash(self, stock_num: int, max_tick: int, init_cash: float):
        self.item.set_holdings(
            holdings=np.zeros((max_tick + 1, stock_num), dtype=int))
        self.item.set_init_cash(init_cash=init_cash)
        self.item.set_cash(
            cash=np.zeros(max_tick + 1, dtype=int))
        self.item.set_stock_value(
            stock_value=np.zeros(max_tick + 1, dtype=int))
        return self

    def with_clock(self, clock: Clock):
        self.item.set_clock(clock=clock)
        return self

    def with_all_close(self, get_all_close: Wrapper):
        self.item.set_all_close(get_all_close=get_all_close)
        return self

    def build(self):
        self.item.__class__ = Account
        return self.item
