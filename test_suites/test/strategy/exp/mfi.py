import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pylab import mpl
from datetime import datetime, timedelta
# 引入TA-Lib库
import talib as ta
import time
import akshare as ak

mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False


def plot_shangzheng():
    from_date = '2010-01-01'
    current_dt = time.strftime("%Y-%m-%d", time.localtime())
    # current_dt = datetime.strptime(current_dt, '%Y-%m-%d')
    df = ak.stock_zh_a_daily(symbol='sh000001', start_date=from_date, end_date=current_dt)
    df.index = pd.to_datetime(df.date)
    df = df.sort_index()
    df['ret'] = df.close / df.close.shift(1) - 1
    high, low, close, volume = df.high.values, df.low.values, df.close.values, df.volume.values
    df['mfi'] = ta.MFI(high, low, close, volume, timeperiod=14)
    plt.figure(figsize=(16, 14))
    plt.subplot(211)
    df['close'].plot(color='r')
    plt.xlabel('')
    plt.title('上证综指走势', fontsize=15)
    plt.subplot(212)
    df['mfi'].plot()
    plt.title('MFI指标', fontsize=15)
    plt.xlabel('')
    plt.show()

    # 当前日的MFI<20，而当日的MFI>20时，买入信号设置为1
    for i in range(15, len(df)):
        if df['mfi'][i] > 20 and df['mfi'][i - 1] < 20:
            df.loc[df.index[i], '收盘信号'] = 1
        if df['mfi'][i] < 80 and df['mfi'][i - 1] > 80:
            df.loc[df.index[i], '收盘信号'] = 0

    # 计算每天的仓位，当天持有上证指数时，仓位为1，当天不持有上证指数时，仓位为0
    pd.options.mode.chained_assignment = None
    df['当天仓位'] = df['收盘信号'].shift(1)
    df['当天仓位'].fillna(method='ffill', inplace=True)

    d = df[df['当天仓位'] == 1].index[0] - timedelta(days=1)
    df_new = df.loc[d:]
    df_new['ret'][0] = 0
    df_new['当天仓位'][0] = 0

    # 当仓位为1时，买入上证指数，当仓位为0时，空仓，计算资金指数
    df_new['资金指数'] = (df_new.ret * df['当天仓位'] + 1.0).cumprod()
    df_new['指数净值'] = (df_new.ret + 1.0).cumprod()

    df.close.plot(figsize=(16, 7))
    for i in range(len(df)):
        if df['收盘信号'][i] == 1:
            plt.annotate('买', xy=(df.index[i], df.close[i]), arrowprops=dict(facecolor='r', shrink=0.05))
        if df['收盘信号'][i] == 0:
            plt.annotate('卖', xy=(df.index[i], df.close[i]), arrowprops=dict(facecolor='g', shrink=0.1))
    plt.title('上证指数2000-2019年MFI买卖信号', size=15)
    plt.xlabel('')
    ax = plt.gca()
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    plt.show()

    df1 = df.loc['2016-01-01':, ]
    df1.close.plot(figsize=(16, 7))
    for i in range(len(df1)):
        if df1['收盘信号'][i] == 1:
            plt.annotate('买', xy=(df1.index[i], df1.close[i]), arrowprops=dict(facecolor='r', shrink=0.05))
        if df1['收盘信号'][i] == 0:
            plt.annotate('卖', xy=(df1.index[i], df1.close[i]), arrowprops=dict(facecolor='g', shrink=0.1))
    plt.title('上证指数2016-2022年MFI买卖信号', fontsize=15)
    plt.xlabel('')
    ax = plt.gca()
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    plt.show()

    df1['策略净值'] = (df1.ret * df1['当天仓位'] + 1.0).cumprod()
    df1['指数净值'] = (df1.ret + 1.0).cumprod()

    df1['策略收益率'] = df1['策略净值'] / df1['策略净值'].shift(1) - 1
    df1['指数收益率'] = df1.ret
    total_ret = df1[['策略净值', '指数净值']].iloc[-1] - 1
    annual_ret = pow(1 + total_ret, 250 / len(df_new)) - 1
    dd = (df1[['策略净值', '指数净值']].cummax() - df1[['策略净值', '指数净值']]) / df1[
        ['策略净值', '指数净值']].cummax()
    d = dd.max()
    beta = df1[['策略收益率', '指数收益率']].cov().iat[0, 1] / df1['指数收益率'].var()
    alpha = (annual_ret['策略净值'] - annual_ret['指数净值'] * beta)
    exReturn = df1['策略收益率'] - 0.03 / 250
    sharper_ratio = np.sqrt(len(exReturn)) * exReturn.mean() / exReturn.std()
    TA1 = round(total_ret['策略净值'] * 100, 2)
    TA2 = round(total_ret['指数净值'] * 100, 2)
    AR1 = round(annual_ret['策略净值'] * 100, 2)
    AR2 = round(annual_ret['指数净值'] * 100, 2)
    MD1 = round(d['策略净值'] * 100, 2)
    MD2 = round(d['指数净值'] * 100, 2)
    S = round(sharper_ratio, 2)
    print(
        f'累计收益率：策略{TA1}%，指数{TA2}%;\n'
        f'年化收益率：策略{AR1}%，指数{AR2}%；\n'
        f'最大回撤：  策略{MD1}%，指数{MD2}%;\n'
        f'策略alpha: {round(alpha, 2)}，策略beta：{round(beta, 2)}; \n'
        f'夏普比率：  {S}')

    df1[['策略净值', '指数净值']].plot(figsize=(15, 7))
    plt.title('上证指数与MFI指标策略\n2016年1月1日至今', size=15)

    bbox = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.9)

    plt.text('2017-05-01', 0.75,
             f'累计收益率：策略{TA1}%，指数{TA2}%;\n'
             f'年化收益率：策略{AR1}%，指数{AR2}%;\n'
             f'最大回撤：策略{MD1}%，指数{MD2}%;\n'
             f'策略alpha: {round(alpha, 2)}，策略beta:  {round(beta, 2)}; \n'
             f'夏普比率：  {S}',
             size=13, bbox=bbox)
    plt.xlabel('')
    ax = plt.gca()
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    plt.show()


# plot_shangzheng()


# 计算方法
#
# 1.典型价格（TP）=当日最高价、最低价与收盘价的算术平均值
#
# 2.货币流量（MF）=典型价格（TP）×N日内成交量
#
# 3.如果当日MF>昨日MF，则将当日的MF值视为正货币流量（PMF）
#
# 4.如果当日MF<昨日MF，则将当日的MF值视为负货币流量（NMF）
#
# 5.MFI=100-[100/(1+PMF/NMF)]
#
# 6.参数N一般设为14日。
#
# 应用法则
#
# 1.显示超买超卖是MFI指标最基本的功能。当MFI>80时为超买，在其回头向下跌破80时，为短线卖出时机。
#
# 2.当MFI<20时为超卖，当其回头向上突破20时，为短线买进时机。
#
# 3.当MFI>80，而产生背离现象时，视为卖出信号。
#
# 4.当MFI<20，而产生背离现象时，视为买进信号。
#
# 注意要点
#
# 1.经过长期测试，MFI指标的背离讯号更能忠实的反应股价的反转现象。一次完整的波段行情，至少都会维持一定相当的时间，反转点出现的次数并不会太多。
#
# 2.将MFI指标的参数设定为14天时，其背离讯号产生的时机，大致上都能和股价的顶点吻合。因此在使用MFI指标时，参数设定方面应尽量维持14日的原则。
# 熔融流动指数:MFI,无纺布熔融喷丝中常用参数。


def get_data(code, date):
    # df=ts.get_k_data(code,start=date)
    from_date = date
    from_date = datetime.strptime(from_date, "%Y-%m-%d")
    current_dt = time.strftime("%Y-%m-%d", time.localtime())
    current_dt = datetime.strptime(current_dt, '%Y-%m-%d')
    df = ak.stock_zh_a_daily(symbol='sh000001', start_date=from_date, end_date=current_dt)
    df.index = pd.to_datetime(df.date)
    df = df.sort_index()
    df['ret'] = df.close / df.close.shift(1) - 1
    return df


# 关掉pandas的warnings
pd.options.mode.chained_assignment = None


def strategy(code, date, L, H):
    df = get_data(code, date)
    high, low, close, volume = df.high.values, df.low.values, df.close.values, df.volume.values
    df['mfi'] = ta.MFI(high, low, close, volume, timeperiod=14)
    for i in range(14, len(df)):
        if df['mfi'][i] > L > df['mfi'][i - 1]:
            df.loc[df.index[i], '收盘信号'] = 1
        if df['mfi'][i] < H < df['mfi'][i - 1]:
            df.loc[df.index[i], '收盘信号'] = 0

    df['当天仓位'] = df['收盘信号'].shift(1)
    df['当天仓位'].fillna(method='ffill', inplace=True)

    d = df[df['当天仓位'] == 1].index[0] - timedelta(days=1)
    df1 = df.loc[d:]
    df1['ret'][0] = 0
    df1['当天仓位'][0] = 0
    # 当仓位为1时，买入上证指数，当仓位为0时，空仓，计算资金指数
    df1['策略净值'] = (df1.ret.values * df1['当天仓位'].values + 1.0).cumprod()
    df1['指数净值'] = (df1.ret.values + 1.0).cumprod()
    df1['策略收益率'] = df1['策略净值'] / df1['策略净值'].shift(1) - 1
    df1['指数收益率'] = df1.ret
    total_ret = df1[['策略净值', '指数净值']].iloc[-1] - 1
    annual_ret = pow(1 + total_ret, 250 / len(df1)) - 1
    dd = (df1[['策略净值', '指数净值']].cummax() - df1[['策略净值', '指数净值']]) / \
         df1[['策略净值', '指数净值']].cummax()
    d = dd.max()
    beta = df1[['策略收益率', '指数收益率']].cov().iat[0, 1] / df1['指数收益率'].var()
    alpha = (annual_ret['策略净值'] - annual_ret['指数净值'] * beta)
    exReturn = df1['策略收益率'] - 0.03 / 250
    sharper_atio = np.sqrt(len(exReturn)) * exReturn.mean() / exReturn.std()
    TA1 = round(total_ret['策略净值'] * 100, 2)
    TA2 = round(total_ret['指数净值'] * 100, 2)
    AR1 = round(annual_ret['策略净值'] * 100, 2)
    AR2 = round(annual_ret['指数净值'] * 100, 2)
    MD1 = round(d['策略净值'] * 100, 2)
    MD2 = round(d['指数净值'] * 100, 2)
    S = round(sharper_atio, 2)
    df1[['策略净值', '指数净值']].plot(figsize=(15, 7))
    plt.title('上证指数与MFI指标策略\n' + date + '至今', size=15)
    bbox = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.9)
    plt.text(df1.index[int(len(df1) / 5)], df1['指数净值'].max() / 1.5,
             f'累计收益率：策略{TA1}%，指数{TA2}%;\n年化收益率：策略{AR1}%，指数{AR2}%；\n最大回撤：  策略{MD1}%，指数{MD2}%;\n策略alpha: {round(alpha, 2)}，策略beta：{round(beta, 2)}; \n夏普比率：  {S}',
             size=13, bbox=bbox)
    plt.xlabel('')
    ax = plt.gca()
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    plt.show()


if __name__ == '__main__':
    strategy('sh', '2009-05-12', 20, 95)
'''
strategy('sh', '2009-05-12', 20, 80)
strategy('sh', '2009-04-12', 20, 90)
strategy('sh', '2009-04-12', 20, 95)
strategy('sh', '2009-04-12', 30, 95)
strategy('sh', '2009-04-12', 15, 95)
strategy('sh', '2016-01-01', 20, 90)
strategy('sh', '2000-01-01', 20, 80)
strategy('sh', '2000-01-01', 20, 92)
strategy('sh', '2017-04-12', 20, 80)
strategy('sh', '2017-04-12', 20, 92)
strategy('cyb', '2017-04-01', 20, 80)
'''
