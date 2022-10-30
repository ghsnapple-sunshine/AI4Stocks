import backtrader as bt
import pandas as pd
import akshare as ak
from datetime import datetime
import quantstats
import webbrowser
import warnings

from buffett.download.slow.ak_daily_handler import AkDailyHandler
from buffett.download.mysql.types import RoleType
from buffett.download.mysql.operator import Operator
from test.backtrader.sample.strategy.stamp import StampDutyCommissionScheme
from test.backtrader.sample.strategy.multi_forks import StrategyMultiForks

# 创建cerebro实体
cerebro = bt.Cerebro()
# 添加策略
cerebro.addstrategy(StrategyMultiForks)

# 获取股票池数据

# code_list = ['000001', '000002', '000004'] '000004'有未知问题，暂时屏蔽
code_list = ['000001', '000002']
hdl = AkDailyHandler(Operator(RoleType.DbTest))
for code in code_list:
    # 利用 AKShare 获取股票的后复权数据，这里只获取前 6 列
    stock_hfq_df = ak.stock_zh_a_hist(symbol=code, adjust="hfq").iloc[:, :6]
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
    # 将数据长度不足的股票删去
    if len(stock_hfq_df) < 55:
        continue
    # else
    data = bt.feeds.PandasData(
        dataname=stock_hfq_df,
        fromdate=datetime(2010, 1, 1),
        todate=datetime(2010, 12, 31))  # 加载数据
    cerebro.adddata(data, name=code)

# 设置启动资金
cerebro.broker.setcash(1000000)
# 设置交易单位大小
cerebro.addsizer(bt.sizers.FixedSize, stake=100)
# 设置印花税和佣金
comminfo = StampDutyCommissionScheme(stamp_duty=0.001, commission=0.0003)
cerebro.broker.addcommissioninfo(comminfo)
# 不显示曲线
# for d in cerebro.datas:
#    d.plotinfo.plot = False
# 打印开始信息
print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

# 查看策略效果
cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
back = cerebro.run(exactbars=True, stdstats=False)

warnings.filterwarnings('ignore')
strat = back[0]
portfolio_stats = strat.analyzers.getbyname('pyfolio')
returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
returns.index = returns.index.tz_convert(None)

quantstats.reports.html(returns, output='stats.html', title='Stock Sentiment')

f = webbrowser.open('stats.html')
# 打印最后结果
print('Final Profolio Value : %.2f' % cerebro.broker.getvalue())


