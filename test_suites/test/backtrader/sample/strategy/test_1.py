from datetime import datetime
import backtrader as bt
import akshare as ak
import pandas as pd
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo
from test.backtrader.sample.strategy.forks import StrategyForks
import os


def get_pnl_gross(strats):
    a = strats[0].analyzers.tradeanalyzer.get_analysis()
    return a.pnl.gross.total if 'pnl' in a else 0


if __name__ == '__main__':
    # 利用 AKShare 获取股票的后复权数据，这里只获取前 6 列
    stock_hfq_df = ak.stock_zh_a_hist(symbol="000001", adjust="hfq").iloc[:, :6]
    # 处理字段命名，以符合 Backtrader 的要求
    stock_hfq_df.columns = [
        'date',
        'open',
        'close',
        'high',
        'low',
        'volume',
    ]
    # 把 date 作为日期索引，以符合 Backtrader 的要求
    stock_hfq_df.index = pd.to_datetime(stock_hfq_df['date'])

    cerebro = bt.Cerebro()  # 初始化回测系统
    start_date = datetime(2010, 1, 1)  # 回测开始时间
    end_date = datetime(2020, 1, 1)  # 回测结束时间
    data = bt.feeds.PandasData(
        dataname=stock_hfq_df,
        fromdate=start_date,
        todate=end_date)  # 加载数据
    cerebro.adddata(data)  # 将数据传入回测系统
    # cerebro.addstrategy(StrategySMA20)  # 将交易策略加载到回测系统中
    cerebro.addstrategy(StrategyForks)  # 将交易策略加载到回测系统中
    start_cash = 1000000
    cerebro.broker.setcash(start_cash)  # 设置初始资本为 1,000,000
    cerebro.broker.setcommission(commission=0.0003)  # 设置交易手续费为 0.03%
    result = cerebro.run()  # 运行回测系统
    port_value = cerebro.broker.getvalue()  # 获取回测结束后的总资金
    pnl = port_value - start_cash  # 盈亏统计
    print(f"初始资金: {start_cash}\n回测期间：{start_date.strftime('%Y/%m/%d')}-{end_date.strftime('%Y/%m/%d')}")
    print(f"总资金: {round(port_value, 2)}")
    print(f"净收益: {round(pnl, 2)}")
    plot_config = {
        'id:ind#0': dict(
            subplot=False,
        ),
    }

    b = Bokeh(style='bar',
              scheme=Tradimo(),
              tabs='multi',
              plot_config=plot_config)

    cerebro.adddata(data, name='000001')
    cerebro.plot(b)

    # 复制结果到目标目录
    path = 'D:\\TestResults\\%s\\' % datetime.now().strftime('%Y%m%d_%H%M%S')
    os.makedirs(path)  # os.mkdir只能创建一层目录
    os.system('chcp 65001')
    os.system('copy C:\\Users\\twinkle-pc\\AppData\\Local\\Temp\\bt_bokeh_plot_0.html %sresult.html' % path)
