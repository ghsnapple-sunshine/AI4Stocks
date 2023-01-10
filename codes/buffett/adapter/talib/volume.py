from talib import AD, ADOSC, OBV

from buffett.adapter.numpy import ndarray


class Volume:
    @staticmethod
    def chalkin_ad_line(high: ndarray, low: ndarray, close: ndarray, volume: ndarray):
        """
        名称：Chaikin A/D Line 累积/派发线（Accumulation/Distribution Line）
        简介：Marc Chaikin提出的一种平衡交易量指标，以当日的收盘价位来估算成交流量，用于估定一段时间内该证券累积的资金流量。

        研判：
        1、A/D测量资金流向，向上的A/D表明买方占优势，而向下的A/D表明卖方占优势
        2、A/D与价格的背离可视为买卖信号，即底背离考虑买入，顶背离考虑卖出
        3、应当注意A/D忽略了缺口的影响，事实上，跳空缺口的意义是不能轻易忽略的
        A/D指标无需设置参数，但在应用时，可结合指标的均线进行分析

        :return:
        """
        real = AD(high, low, close, volume)
        return real.reshape((len(high), 1))

    @staticmethod
    def chalkin_ad_oscillator(
        high: ndarray, low: ndarray, close: ndarray, volume: ndarray
    ):
        """
        名称：Chaikin A/D Oscillator Chaikin震荡指标
        简介：将资金流动情况与价格行为相对比，检测市场中资金流入和流出的情况

        研判：
        1、交易信号是背离：看涨背离做多，看跌背离做空
        2、股价与90天移动平均结合，与其他指标结合
        3、由正变负卖出，由负变正买进

        :return:
        """
        real = ADOSC(high, low, close, volume, fastperiod=3, slowperiod=10)
        return real.reshape((len(high), 1))

    @staticmethod
    def on_balance_volume(close: ndarray, volume: ndarray):
        """
        名称：On Balance Volume 能量潮
        简介：Joe Granville提出，通过统计成交量变动的趋势推测股价趋势

        研判：
        1、以“N”字型为波动单位，一浪高于一浪称“上升潮”，下跌称“跌潮”；上升潮买进，跌潮卖出
        2、须配合K线图走势
        3、用多空比率净额法进行修正，但不知TA-Lib采用哪种方法

        :param close:
        :param volume:
        :return:
        """
        real = OBV(close, volume)
        return real.reshape((len(close), 1))
