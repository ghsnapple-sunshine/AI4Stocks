from buffett.adapter.enum import Enum


class OrderStatus(Enum):
    ACTIVE = 1  # 委托中
    DEALED = 2  # 已成交

    OVERDUE = 10  # 超期未成交
    CANCELED = 11  # 已撤销

    INVALID = 20  # 无效


class Order:
    def __init__(
        self,
        code: int,
        order_num: int,
        create_tick: int,
        limit_price: int = -1,
        valid_time: int = 1,
    ):
        """
        初始化订单
        :param code:        股票内部ID
        :param order_num:   委托数
        :param create_tick: 当前时刻
        :param limit_price: 限价（-1为不限价）
        :param valid_time:  订单有效时间
        """
        # 基本信息
        self.__code = code
        self.__order_num = order_num
        self.__limit_price = limit_price
        self._create_tick = create_tick
        self.__valid_time = valid_time
        # 状态字
        self._status = OrderStatus.INVALID if order_num == 0 else OrderStatus.ACTIVE
        # 交易信息
        self._deal_num = 0
        self._first_deal_time = -1
        self._last_deal_time = -1
        self._deal_price = 0
        self._charge = 0

    @property
    def order_num(self):
        return self.__order_num

    @property
    def resi_deal_num(self):
        return self.__order_num - self._deal_num

    @property
    def cje(self):
        return self._deal_num * self._deal_price

    def set_dealed(self, deal_num: int, deal_time: int, deal_price: int) -> None:
        if deal_num * self.__order_num <= 0 or abs(deal_num) > abs(self.resi_deal_num):
            err_msg = "Incorrect deal: order wishes yet {0} while deal is {1}".format(
                self.resi_deal_num, deal_num
            )
            raise ValueError(err_msg)

        if deal_num == self.__order_num:  # 一次性成交
            self._status = OrderStatus.DEALED
            self._first_deal_time = deal_time
            self._last_deal_time = deal_time
            self._deal_num = deal_num
            self._deal_price = deal_price
            return

        # 部分成交（因为当前OrdMan未考虑成交量，因此一定是全部成交）
        self._first_deal_time = (
            self._first_deal_time if self._first_deal_time > 0 else deal_time
        )
        self._last_deal_time = deal_time
        self._deal_price = (
            self._deal_price * self._deal_num + deal_price * deal_num
        ) / (self._deal_num + deal_num)
        self._deal_num = self._deal_num + deal_num

    def set_charge(self, charge: int) -> None:
        self._charge = charge

    def set_overdue(self) -> None:
        self._status = OrderStatus.OVERDUE

    def set_canceled(self) -> None:
        self._status = OrderStatus.CANCELED

    def is_overdue(self, curr_time: int) -> bool:
        return (
            self.__valid_time < 0 or self.__valid_time > curr_time
        )  # valid_time为负值的情况视作无效值

    @property
    def code(self) -> int:
        return self.__code

    @property
    def is_sell(self) -> bool:
        return self.__order_num < 0

    @property
    def is_buy(self) -> bool:
        return self.__order_num > 0

    @property
    def not_empty(self) -> bool:
        return self.__order_num != 0

    @property
    def limit_price(self) -> int:
        return self.__limit_price

    @property
    def is_dealed(self) -> int:
        return self._status == OrderStatus.DEALED

    @property
    def deal_price(self) -> int:
        return self._deal_price
