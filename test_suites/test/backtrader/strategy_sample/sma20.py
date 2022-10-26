from backtrader import Strategy
from backtrader.indicators.sma import MovingAverageSimple


class StrategySMA20(Strategy):
    def log(self, txt, dt=None):
        """ Logging function fot this strategy_sample"""
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.data_close = self.datas[0].close

        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buy_price = None
        self.buy_comm = None
        self.bar_executed = None

        self.sma20 = MovingAverageSimple(self.data_close, period=20)

    def notify_order(self, order):
        if order._status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order._status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buy_price = order.executed.price
                self.buy_comm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order._status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):  # 交易执行后，在这里处理
        if not trade.isclosed:
            return
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))  # 记录下盈利数据。

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.data_close[0])

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # 检查是否在市场
        if not self.position:
            '''
            # 不在，那么连续3天价格下跌就买点
            if self.data_close[0] < self.data_close[-1]:
                # current close less than previous close

                if self.data_close[-1] < self.data_close[-2]:
                    # previous close less than the previous close

                    # BUY, BUY, BUY!!! (with default parameters)
                    self.log('BUY CREATE, %.2f' % self.data_close[0])

                    # Keep track of the created order to avoid a 2nd order
                    self.order = self.buy(size=100)
            '''
            if self.data_close[0] > self.sma20[0]:  # 执行买入条件判断：收盘价格上涨突破20日均线
                self.log('Event close(%.2f) > sma(%.2f)' % (self.data_close[0], self.sma20[0]))
                self.order = self.buy(size=100)  # 执行买入
        else:
            '''
            # 已经在市场，5天后就卖掉。
            if len(self) >= (self.bar_executed + 5):
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.2f' % self.data_close[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell(size=100)
            '''
            if self.data_close[0] < self.sma20[0]:  # 执行卖出条件判断：收盘价格跌破20日均线
                self.log('Event close(%.2f) < sma(%.2f)' % (self.data_close[0], self.sma20[0]))
                self.order = self.sell(size=100)  # 执行卖出
