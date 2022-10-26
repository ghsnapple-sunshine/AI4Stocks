from ai4stocks.backtrader.frame.clock import Clock
from ai4stocks.backtrader.frame.order import Order as Order
from ai4stocks.backtrader.interface import ITimeSequence as Sequence, ITradeSuccessNotify as ITSN
from ai4stocks.backtrader.tools import ChargeCalculator as Calc
from ai4stocks.common.wrapper import Wrapper


class AccountingManager(Sequence):
    def __init__(self):
        self._charge_calc: Calc = Calc()
        self._listeners: list[ITSN] = []
        self._get_holding: Wrapper = Wrapper()
        self._get_curr_olhc: Wrapper = Wrapper()
        # self._clock: Clock = Clock()

    def run(self):
        pass

    def trade_success(self, order: Order):
        for li in self._listeners:
            li.trade_success(order)

    def handle_order(self, order: Order):
        if order.is_buy:
            self.__handle_buy_order__(order=order)
        elif order.is_sell:
            self.__handle_sell_order__(order=order)

    # region 私有方法
    def __handle_buy_order__(self, order: Order):
        """
        处理买入订单
        注意，order的buy操作只有在下单时刻的下个tick才会触发。
        :param order:   订单
        :return:        None（如下单，刷新订单信息）
        """
        _open, low, high, cjl = self._get_curr_olhc.run(code=order.code)
        deal_price: int
        deal_num = min(order.order_num, cjl)  # 卖出考虑了交易额，持仓
        if order.limit_price > 0:  # 限价单
            if order.limit_price >= _open:
                deal_price = _open
            elif high >= order.limit_price > _open:
                deal_price = order.limit_price
            else:
                return  # 跳过set_dealed
        else:
            # 始终以开盘价成交
            deal_price = _open
        order.set_dealed(
            deal_num=deal_num,
            deal_time=self.curr_tick,
            deal_price=deal_price)
        if order.is_dealed:  # 如果完全成交
            order.set_charge(charge=self._charge_calc.run(order.cje))
            self.trade_success(order=order)

    # 注意，order的buy和sold操作只有在下单时刻的下个tick才会触发。
    def __handle_sell_order__(self, order: Order):
        """
        处理卖出订单
        注意，order的buy操作只有在下单时刻的下个tick才会触发。
        :param order:   订单
        :return:        None（如下单，刷新订单信息）
        """
        _open, low, high, cjl = self._get_curr_olhc.run(code=order.code)
        holding = self._get_holding.run(code=order.code)
        deal_price: int
        deal_num = max(-holding, order.order_num, -cjl)  # 卖出考虑了交易额，持仓
        if order.limit_price > 0:  # 限价单
            if order.limit_price >= _open:
                deal_price = _open
            elif high >= order.limit_price > _open:
                deal_price = order.limit_price
            else:
                return  # 跳过set_dealed
        else:
            # 始终以开盘价成交
            deal_price = _open
        order.set_dealed(
            deal_num=deal_num,
            deal_time=self.curr_tick,
            deal_price=deal_price)
        if order.is_dealed:  # 如果完全成交
            order.set_charge(charge=self._charge_calc.run(order.cje))
            self.trade_success(order=order)
            return order


# endregion

# region builder
class AccountManagerBuilder:
    class AccountManagerUnderBuilt(AccountingManager):
        def set_charge_calc(self, charge_calc: Calc):
            self._charge_calc = charge_calc

        def set_listeners(self, listeners: list[ITSN]):
            self._listeners = listeners

        def set_holding(self, get_holding: Wrapper):
            self._get_holding = get_holding

        def set_olhc(self, get_curr_olhc: Wrapper):
            self._get_curr_olhc = get_curr_olhc

        def set_clock(self, clock: Clock):
            self._clock = clock

    def __init__(self):
        self.item = AccountManagerBuilder.AccountManagerUnderBuilt()

    def with_charge_calc(self, charge_calc: Calc):
        self.item.set_charge_calc(charge_calc=charge_calc)
        return self

    def with_listeners(self, listeners: list[ITSN]):
        self.item.set_listeners(listeners=listeners)
        return self

    def with_holding(self, get_holding: Wrapper):
        self.item.set_holding(get_holding=get_holding)
        return self

    def with_olhc(self, get_curr_olhc: Wrapper):
        self.item.set_olhc(get_curr_olhc=get_curr_olhc)
        return self

    def with_clock(self, clock: Clock):
        self.item.set_clock(clock=clock)
        return self

    def build(self):
        self.item.__class__ = AccountingManager
        return self.item
# endregion
