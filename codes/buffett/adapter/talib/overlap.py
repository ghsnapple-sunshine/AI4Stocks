from talib import (
    BBANDS,
    DEMA,
    EMA,
    HT_TRENDLINE,
    KAMA,
    MA,
    MAMA,
    MAVP,
    MIDPOINT,
    MIDPRICE,
    SAR,
    SAREXT,
    SMA,
    T3,
    TEMA,
    WMA,
)

from buffett.adapter.numpy import ndarray, np


class Overlap:
    @staticmethod
    def bollinger_bands(close: ndarray, timeperiod=5) -> ndarray:
        """
        名称：布林线
        简介：其利用统计原理，求出股价的标准差及其信赖区间，从而确定股价的波动范围及未来走势，利用波带显示股价的安全高低价位，因而也被称为布林带。

        :param close:
        :param timeperiod:
        :return:
        """
        upperband, middleband, lowerband = BBANDS(
            close, timeperiod=timeperiod, nbdevup=2, nbdevdn=2, matype=0
        )
        return np.concatenate([upperband, middleband, lowerband]).reshape(
            (len(close), 3), order="F"
        )

    @staticmethod
    def double_exponential_moving_average(close: ndarray, timeperiod=30) -> ndarray:
        """
        名称：双移动平均线
        简介：两条移动平均线来产生趋势信号，较长期者用来识别趋势，较短期者用来选择时机。正是两条平均线及价格三者的相互作用，才共同产生了趋势信号。

        :param close:
        :param timeperiod:
        :return:
        """
        real = DEMA(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def exponential_moving_average(close: ndarray, timeperiod=30) -> ndarray:
        """
        名称：指数平均数
        简介：是一种趋向类指标，其构造原理是仍然对价格收盘价进行算术平均，并根据计算结果来进行分析，用于判断价格未来走势的变动趋势。

        :param close:
        :param timeperiod:
        :return:
        """
        real = EMA(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def hilbert_transform_instantaneous_trendline(close: ndarray) -> ndarray:
        """
        名称：希尔伯特瞬时变换
        简介：是一种趋向类指标，其构造原理是仍然对价格收盘价进行算术平均，并根据计算结果来进行分析，用于判断价格未来走势的变动趋势。

        :param close:
        :return:
        """
        real = HT_TRENDLINE(close)
        return real.reshape((len(close), 1))

    @staticmethod
    def kaufman_adaptive_moving_average(close: ndarray, timeperiod=30) -> ndarray:
        """
        名称：考夫曼自适应移动平均线
        简介：短期均线贴近价格走势，灵敏度高，但会有很多噪声，产生虚假信号；长期均线在判断趋势上一般比较准确 ，但是长期均线有着严重滞后的问题。
        我们想得到这样的均线，当价格沿一个方向快速移动时，短期的移动 平均线是最合适的；当价格在横盘的过程中，长期移动平均线是合适的。

        :param close:
        :param timeperiod:
        :return:
        """
        real = KAMA(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def moving_average(close: ndarray, timeperiod=5) -> ndarray:
        """
        名称：移动平均线
        简介：移动平均线，Moving Average，简称MA，原本的意思是移动平均，由于我们将其制作成线形，所以一般称之为移动平均线，简称均线。
        它是将某一段时间的收盘价之和除以该周期。 比如日线MA5指5天内的收盘价除以5 。

        :param close:
        :param timeperiod:
        :return:
        """
        real = MA(close, timeperiod=timeperiod, matype=0)
        return real.reshape((len(close), 1))

    @staticmethod
    def mesa_adaptive_moving_average(close: ndarray) -> ndarray:
        """
        MAMA

        :param close:
        :return:
        """
        mama, fama = MAMA(close, fastlimit=0, slowlimit=0)
        return np.concatenate([mama, fama]).reshape((len(close), 2), order="F")

    @staticmethod
    def moving_average_with_variable_period(
        close: ndarray, periods: ndarray
    ) -> ndarray:
        """
        MAVP

        :param close:
        :param periods:
        :return:
        """
        real = MAVP(close, periods, minperiod=2, maxperiod=30, matype=0)
        return real.reshape((len(close), 1))

    @staticmethod
    def midpoint_over_period(close: ndarray, timeperiod=14) -> ndarray:
        """
        MIDPOINT

        :param close:
        :param timeperiod:
        :return:
        """
        real = MIDPOINT(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def midpoint_price_over_period(high: ndarray, low: ndarray, timeperiod=14):
        """
        MIDPRICE

        :param high:
        :param low:
        :param timeperiod:
        :return:
        """
        real = MIDPRICE(high, low, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def parabolic_sar(high: ndarray, low: ndarray):
        """
        名称：抛物线指标
        简介：抛物线转向也称停损点转向，是利用抛物线方式，随时调整停损点位置以观察买卖点。由于停损点（又称转向点SAR）以弧形的方式移动，故称之为抛物线转向指标。

        :param high:
        :param low:
        :return:
        """
        real = SAR(high, low, acceleration=0, maximum=0)
        return real.reshape((len(high), 1))

    @staticmethod
    def parabolic_sar_extended(high: ndarray, low: ndarray):
        """
        SAR-EXT

        :param high:
        :param low:
        :return:
        """
        real = SAREXT(
            high,
            low,
            startvalue=0,
            offsetonreverse=0,
            accelerationinitlong=0,
            accelerationlong=0,
            accelerationmaxlong=0,
            accelerationinitshort=0,
            accelerationshort=0,
            accelerationmaxshort=0,
        )
        return real.reshape((len(high), 1))

    @staticmethod
    def simple_moving_average(close: ndarray, timeperiod=30):
        """
        名称：简单移动平均线
        简介：移动平均线，Moving Average，简称MA，原本的意思是移动平均，由于我们将其制作成线形，所以一般称之为移动平均线，简称均线。
        它是将某一段时间的收盘价之和除以该周期。 比如日线MA5指5天内的收盘价除以5 。

        :param close:
        :param timeperiod:
        :return:
        """
        real = SMA(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def triple_exponential_moving_average(close: ndarray, timeperiod=5):
        """
        名称：三重指数移动平均线
        简介：TRIX长线操作时采用本指标的讯号，长时间按照本指标讯号交易，获利百分比大于损失百分比，利润相当可观。 比如日线MA5指5天内的收盘价除以5 。

        :param close:
        :param timeperiod:
        :return:
        """
        real = T3(close, timeperiod=timeperiod, vfactor=0)
        return real.reshape((len(close), 1))

    @staticmethod
    def triple_exponential_moving_average2(close: ndarray, timeperiod=5):
        """
        TEMA

        :param close:
        :param timeperiod:
        :return:
        """
        real = TEMA(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def triangular_moving_average(close: ndarray, timeperiod=30):
        """
        名称：加权移动平均线
        简介：移动加权平均法是指以每次进货的成本加上原有库存存货的成本，除以每次进货数量与原有库存存货的数量之和，据以计算加权平均单位成本，
        以此为基础计算当月发出存货的成本和期末存货的成本的一种方法。

        :param close:
        :param timeperiod:
        :return:
        """
        real = WMA(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))
