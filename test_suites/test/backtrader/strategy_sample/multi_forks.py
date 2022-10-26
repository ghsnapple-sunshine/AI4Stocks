import backtrader as bt
from backtrader.indicators import MovingAverageSimple, CrossOver


# 创建策略
class StrategyMultiForks(bt.Strategy):
    params = (
        ('maperiod1', 5),
        ('maperiod2', 13),
        ('maperiod3', 21),
        ('maperiod4', 34),
        ('maperiod5', 55),
        ('printlog', True),
        ('poneplot', False),  # 是否打印到同一张图
        ('pstake', 100000)  # 单笔交易股票数据
    )

    def log(self, txt, dt=None, doprint=False):
        dt = dt or self.datas[0].datetime.date(0)
        # print('%s,%s' % (dt.isoformat(),txt))

        """策略的日志函数"""
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s,%s' % (dt.isoformat(), txt))

    def __init__(self):
        self.inds = dict()
        for i, d in enumerate(self.datas):
            self.inds[d] = dict()
            self.inds[d]['ma1'] = MovingAverageSimple(d.close, period=self.params.maperiod1)
            self.inds[d]['ma2'] = MovingAverageSimple(d.close, period=self.params.maperiod2)
            self.inds[d]['ma3'] = MovingAverageSimple(d.close, period=self.params.maperiod3)
            self.inds[d]['ma4'] = MovingAverageSimple(d.close, period=self.params.maperiod4)
            self.inds[d]['ma5'] = MovingAverageSimple(d.close, period=self.params.maperiod5)
            self.inds[d]['D1'] = CrossOver(self.inds[d]['ma5'], self.inds[d]['ma4'])  # 交叉信号
            self.inds[d]['A1'] = CrossOver(self.inds[d]['ma1'], self.inds[d]['ma2'])  # 交叉信号
            self.inds[d]['C1'] = CrossOver(self.inds[d]['ma2'], self.inds[d]['ma3'])
            # 跳过第一只股票data，第一只股票data作为主图数据pan
            if i > 0:
                if self.p.poneplot:
                    d.plotinfo.plotmaster = self.datas[0]

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('OPERATION PROFIT,GROSS %.2F,NET %.2F' %
                 (trade.pnl, trade.pnlcomm))

    # 多股回测时使用，数据读取。
    def prenext(self):
        self.next()

    def next(self):
        # 获取当天日期
        date = self.datas[0].datetime.date(0)
        # 获取当天value
        value = self.broker.getvalue()
        for i, d in enumerate(self.datas):
            dt, dn = self.datetime.date(), d._name  # 获取时间及股票代码
            print(dt, dn)
            pos = self.getposition(d).size
            sig1 = ((self.inds[d]['D1'][-1] > 0) and (self.inds[d]['A1'][0] > 0)) and (
                    self.inds[d]['ma2'][0] > self.inds[d]['ma4'][0]) and (
                           self.inds[d]['ma4'][0] > self.inds[d]['ma4'][-1])
            sig2 = ((self.inds[d]['D1'][-1] > 0) or (self.inds[d]['A1'][0] > 0)) and (
                    self.inds[d]['ma2'][0] > self.inds[d]['ma2'][-1]) and (d.close[0] / d.initial[0] > 1.05) and (
                           d.volume[0] / d.volume[-1] > 2)
            sig3 = ((self.inds[d]['D1'][-1] > 0) or (self.inds[d]['A1'][0] > 0)) and (
                    self.inds[d]['ma2'][0] > self.inds[d]['ma3'][0]) and (
                           self.inds[d]['ma3'][0] > self.inds[d]['ma4'][0]) and (
                           self.inds[d]['ma4'][0] > self.inds[d]['ma4'][-1])
            sig4 = self.inds[d]['C1'][0] < 0
            # print('sig1',sig1)
            if not pos:  # 不在场内，则可以买入  vol成交量， ref日前
                if sig1 or sig2 and sig3:  # 如果金叉
                    self.buy(data=d, size=self.p.pstake)  # 买
                    self.log('%s,BUY CREATE, %.2f ,%s' % (dt, d.close[0], dn))
                    # self.order = self.buy()
            elif sig4:  # 在场内。且死叉
                self.close(data=d)  # 卖
                self.log('%s,SELL CREATE,%.2f,%s' % (dt, d.close[0], dn))
                # self.order = self.sell()
