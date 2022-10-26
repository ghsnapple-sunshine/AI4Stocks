from ai4stocks.backtrader.frame.clock import Clock
from ai4stocks.backtrader.frame.order import Order
from ai4stocks.backtrader.interface.time_sequence import ITimeSequence as ICN
from ai4stocks.common.wrapper import Wrapper


class OrderManager(ICN):
    def __init__(self):
        """
        初始化OrderManager
        """
        self._orders: list[Order] = []
        self._order_idx: int = -1
        self._active_orders: list[int] = []
        self._now: Clock = Clock()
        self._handle_order: Wrapper = Wrapper()

    def run(self):
        self.__distribute_orders__()  # TODO：原则上先卖后买

    def add_order(self, order: Order) -> None:
        """
        新增委托

        :param order:   委托
        :return:        None
        """
        self._order_idx += 1
        self._orders.append(order)
        if order.not_empty:
            self._active_orders.append(self._order_idx)

    def __distribute_orders__(self):  # 是否销毁已经处理过的订单？
        """
        分发订单

        :return:        None
        """
        new_active_orders: list[int] = []
        for idx in self._active_orders:
            order: Order = self._orders[idx]
            if order.is_overdue(self.curr_tick):  # 过期订单不做罚款处理
                order.set_overdue()
                continue
            self._handle_order.run(order=order)
            new_active_orders.append(idx)
        self._orders = new_active_orders

    '''
    def __handle_order__(self, order: Order) -> None:
        """
        请会计处理订单
        :param order:   订单
        :return:        None
        """
    '''


class OrderManagerBuilder:
    class OrderManagerUnderBuilt(OrderManager):
        def set_handle_order(self, handle_order: Wrapper):
            self._handle_order = handle_order

        def set_clock(self, clock: Clock):
            self._now = clock

    def __init__(self):
        self.item = self.OrderManagerUnderBuilt()

    def with_handle_order(self, handle_order: Wrapper):
        self.item.set_handle_order(handle_order=handle_order)
        return self

    def with_clock(self, clock: Clock):
        self.item.set_clock(clock=clock)
        return self

    def build(self):
        self.item.__class__ = OrderManager
        return self.item
