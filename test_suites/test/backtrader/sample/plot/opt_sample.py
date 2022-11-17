import datetime

import akshare as ak
import backtrader as bt
import pandas as pd
from backtrader_plotting import Bokeh, OptBrowser
from backtrader_plotting.schemes import Tradimo


class MyStrategy(bt.Strategy):
    params = (
        ("buydate", 21),
        ("holdtime", 20),
    )

    def __init__(self):
        sma1 = bt.indicators.SMA(period=11, subplot=True)
        bt.indicators.SMA(period=17, plotmaster=sma1)
        bt.indicators.RSI()

    def next(self):
        pos = len(self.data)
        if pos == self.p.buydate:
            self.buy(self.datas[0], size=None)

        if pos == self.p.buydate + self.p.holdtime:
            self.sell(self.datas[0], size=None)


if __name__ == "__main__":
    cerebro = bt.Cerebro()

    # 利用 AKShare 获取股票的后复权数据，这里只获取前 6 列
    stock_hfq_df = ak.stock_zh_a_hist(symbol="000001", adjust="hfq").iloc[:, :6]
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
    stock_hfq_df.index = pd.to_datetime(stock_hfq_df["date"])

    data = bt.feeds.PandasData(
        dataname=stock_hfq_df,
        fromdate=datetime.datetime(2000, 1, 1),
        todate=datetime.datetime(2001, 2, 28),
    )
    cerebro.adddata(data)
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer)

    cerebro.optstrategy(MyStrategy, buydate=range(40, 180, 30))

    result = cerebro.run(optreturn=True)

    b = Bokeh(style="bar", scheme=Tradimo())
    browser = OptBrowser(b, result)
    browser.start()
