# code source: https://verybadsoldier.github.io/backtrader_plotting/

import datetime

import akshare
import backtrader as bt
import pandas

from backtrader_plotting import Bokeh


class MyStrategy(bt.Strategy):
    def __init__(self):
        sma1 = bt.indicators.SMA(period=11, subplot=True)
        bt.indicators.SMA(period=17, plotmaster=sma1)
        bt.indicators.RSI()

    def next(self):
        pos = len(self.data)
        if pos == 45 or pos == 145:
            self.buy(self.datas[0], size=None)

        if pos == 116 or pos == 215:
            self.sell(self.datas[0], size=None)


if __name__ == "__main__":
    cerebro = bt.Cerebro()

    cerebro.addstrategy(MyStrategy)
    """
    data = bt.feeds.YahooFinanceCSVData(
        dataname="datas/orcl-1995-2014.txt",
        fromdate=datetime.datetime(2000, 1, 1),
        todate=datetime.datetime(2001, 2, 28),
        reverse=False,
        swapcloses=True,
    )
    """
    # 利用 AKShare 获取股票的后复权数据，这里只获取前 6 列
    stock_hfq_df = akshare.stock_zh_a_hist(symbol="000001", adjust="hfq").iloc[:, :6]
    # 处理字段命名，以符合 Backtrader 的要求
    stock_hfq_df.columns = [
        "date",
        "open",
        "close",
        "high",
        "low",
        "volume",
    ]
    # 把 date 作为日期索引，以符合 Backtrader 的要求
    stock_hfq_df.index = pandas.to_datetime(stock_hfq_df["date"])

    data = bt.feeds.PandasData(
        dataname=stock_hfq_df,
        fromdate=datetime.datetime(2000, 1, 1),
        todate=datetime.datetime(2001, 2, 28),
    )

    cerebro.adddata(data)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio)

    cerebro.run()

    b = Bokeh(style="bar", tabs="multi")
    cerebro.plot(b)
