from datetime import datetime

import backtrader as bt
import akshare as ak
import pandas as pd

from ai4stocks.download.slow.ak_stock_daily_handler import AkStockDailyHandler
from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from test.backtrader.strategy_sample.multi_sma import Strategy
from test.backtrader.strategy_sample.stamp import StampDutyCommissionScheme
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo
import os

##########################
# 主程序开始
#########################
cerebro = bt.Cerebro(stdstats=False)
cerebro.addobserver(bt.observers.Broker)
cerebro.addobserver(bt.observers.Trades)
# cerebro.broker.set_coc(True)  # 以订单创建日的收盘价成交
# cerebro.broker.set_coo(True) # 以次日开盘价成交

'''
datadir = './dataswind'  # 数据文件位于本脚本所在目录的data子目录中
datafilelist = glob.glob(os.path.join(datadir, '*'))  # 数据文件路径列表

maxstocknum = 20  # 股票池最大股票数目
# 注意，排序第一个文件必须是指数数据，作为时间基准
datafilelist = datafilelist[0:maxstocknum]  # 截取指定数量的股票池
print(datafilelist)
# 将目录datadir中的数据文件加载进系统


for fname in datafilelist:
    df = pd.read_csv(
        fname,
        skiprows=0,  # 不忽略行
        header=0,  # 列头在0行
    )
    # df = df[~df['交易状态'].isin(['停牌一天'])]  # 去掉停牌日记录
    df['date'] = pd.to_datetime(df['date'])  # 转成日期类型
    df = df.dropna()

    # print(df.info())
    # print(df.head())

    data = PandasDataExtend(
        dataname=df,
        datetime=0,  # 日期列
        open=2,  # 开盘价所在列
        high=3,  # 最高价所在列
        low=4,  # 最低价所在列
        close=5,  # 收盘价价所在列
        volume=6,  # 成交量所在列
        pe=7,
        roe=8,
        marketdays=9,
        openinterest=-1,  # 无未平仓量列
        fromdate=datetime(2002, 4, 1),  # 起始日2002, 4, 1
        todate=datetime(2015, 12, 31),  # 结束日 2015, 12, 31
        plot=False

    )
    ticker = fname[-13:-4]  # 从文件路径名取得股票代码

    cerebro.adddata(data, name=ticker)
'''

code_list = ['000001', '000002']
hdl = AkStockDailyHandler(MysqlOperator(MysqlRole.DbTest))
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
        todate=datetime(2020, 1, 1))  # 加载数据
    cerebro.adddata(data, name=code)

cerebro.addstrategy(Strategy)
startcash = 10000000
cerebro.broker.setcash(startcash)
# 防止下单时现金不够被拒绝。只在执行时检查现金够不够。
cerebro.broker.set_checksubmit(False)
comminfo = StampDutyCommissionScheme(stamp_duty=0.001, commission=0.001)
cerebro.broker.addcommissioninfo(comminfo)
results = cerebro.run()
print('最终市值: %.2f' % cerebro.broker.getvalue())

plot_config = {
    'id:ind#0': dict(
        subplot=False,
    ),
}

b = Bokeh(style='bar',
          scheme=Tradimo(),
          tabs='multi',
          plot_config=plot_config)
cerebro.plot(b)

# 复制结果到目标目录
path = 'D:\\TestResults\\%s\\' % datetime.now().strftime('%Y%m%d_%H%M%S')
os.makedirs(path)  # Note: os.mkdir只能创建一层目录
os.system('chcp 65001')
os.system('copy C:\\Users\\twinkle-pc\\AppData\\Local\\Temp\\bt_bokeh_plot_0.html %sresult.html' % path)
