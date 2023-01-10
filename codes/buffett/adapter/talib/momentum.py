import numpy as np
from talib import (
    ADX,
    ADXR,
    APO,
    AROON,
    AROONOSC,
    BOP,
    CCI,
    CMO,
    DX,
    MACD,
    MACDEXT,
    MFI,
    MINUS_DI,
    MINUS_DM,
    MOM,
    PLUS_DI,
    PLUS_DM,
    PPO,
    ROC,
    ROCR,
    ROCP,
    RSI,
    STOCH,
    STOCHF,
    STOCHRSI,
    TRIX,
    ULTOSC,
    WILLR,
)

from buffett.adapter.numpy import ndarray


class Momentum:
    @staticmethod
    def average_directional_movement_index(
        high: ndarray, low: ndarray, close: ndarray, timeperiod=14
    ) -> ndarray:
        """
        名称：平均趋向指数
        简介：使用ADX指标，指标判断盘整、振荡和单边趋势。

        :param high:
        :param low:
        :param close:
        :param timeperiod:
        :return:
        """
        real = ADX(high, low, close, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def average_directional_movement_index_rating(
        high: ndarray, low: ndarray, close: ndarray, timeperiod=14
    ) -> ndarray:
        """
        名称：平均趋向指数的趋向指数
        简介：使用ADXR指标，指标判断ADX趋势。 NOTE: The ADXR function has an unstable period.

        :param high:
        :param low:
        :param close:
        :param timeperiod:
        :return:
        """
        real = ADXR(high, low, close, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def absolute_price_oscillator(close: ndarray) -> ndarray:
        """
        APO

        :param close:
        :return:
        """
        real = APO(close, fastperiod=12, slowperiod=26, matype=0)
        return real.reshape((len(close), 1))

    @staticmethod
    def aroon(high, low, timeperiod=14) -> ndarray:
        """
        名称：阿隆指标
        简介：该指标是通过计算自价格达到近期最高值和最低值以来所经过的期间数，阿隆指标帮助你预测价格趋势到趋势区域（或者反过来，从趋势区域到趋势）的变化。

        指数应用
        1. 极值0和100
           当UP线达到100时，市场处于强势；如果维持在70~100之间，表示一个上升趋势。
           同样，如果Down线达到0，表示处于弱势，如果维持在0~30之间，表示处于下跌趋势。
        如果两条线同处于极值水平，则表明一个更强的趋势。
        2. 平行运动
        如果两条线平行运动时，表明市场趋势被打破。可以预期该状况将持续下去，只到由极值水平或交叉穿行西安市出方向性运动为止。
        3. 交叉穿行
        当下行线上穿上行线时，表明潜在弱势，预期价格开始趋于下跌。反之，表明潜在强势，预期价格趋于走高。

        :param high:
        :param low:
        :param timeperiod:
        :return:
        """
        aroon_down, aroon_up = AROON(high, low, timeperiod=timeperiod)
        return np.concatenate([aroon_down, aroon_up]).reshape((len(high), 2), order="F")

    @staticmethod
    def aroon_oscillator(high, low, timeperiod=14) -> ndarray:
        """
        阿隆振荡

        :param high:
        :param low:
        :param timeperiod:
        :return:
        """
        real = AROONOSC(high, low, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def balance_of_power(
        open: ndarray, high: ndarray, low: ndarray, close: ndarray
    ) -> ndarray:
        """
        BOP

        :param open:
        :param high:
        :param low:
        :param close:
        :return:
        """
        real = BOP(open, high, low, close)
        return real.reshape((len(open), 1))

    @staticmethod
    def commodity_channel_index(
        high: ndarray, low: ndarray, close: ndarray, timeperiod=14
    ) -> ndarray:
        """
        名称：顺势指标
        简介：CCI指标专门测量股价是否已超出常态分布范围

        指标应用
        1. 当CCI指标曲线在+100线～-100线的常态区间里运行时,CCI指标参考意义不大，可以用KDJ等其它技术指标进行研判。
        2. 当CCI指标曲线从上向下突破+100线而重新进入常态区间时，表明市场价格的上涨阶段可能结束，将进入一个比较长时间的震荡整理阶段，应及时平多做空。
        3. 当CCI指标曲线从上向下突破-100线而进入另一个非常态区间（超卖区）时，表明市场价格的弱势状态已经形成，将进入一个比较长的寻底过程，可以持有空单等待更高利润。
           如果CCI指标曲线在超卖区运行了相当长的一段时间后开始掉头向上，表明价格的短期底部初步探明，可以少量建仓。CCI指标曲线在超卖区运行的时间越长，确认短期的底部的准确度越高。
        4. CCI指标曲线从下向上突破-100线而重新进入常态区间时，表明市场价格的探底阶段可能结束，有可能进入一个盘整阶段，可以逢低少量做多。
        5. CCI指标曲线从下向上突破+100线而进入非常态区间(超买区)时，表明市场价格已经脱离常态而进入强势状态，如果伴随较大的市场交投，应及时介入成功率将很大。
        6. CCI指标曲线从下向上突破+100线而进入非常态区间(超买区)后，只要CCI指标曲线一直朝上运行，表明价格依然保持强势可以继续持有待涨。
           但是，如果在远离+100线的地方开始掉头向下时，则表明市场价格的强势状态将可能难以维持，涨势可能转弱，应考虑卖出。如果前期的短期涨幅过高同时价格回落时交投活跃，则应该果断逢高卖出或做空。
        CCI主要是在超买和超卖区域发生作用，对急涨急跌的行情检测性相对准确。非常适用于股票、外汇、贵金属等市场的短期操作。

        :param high:
        :param low:
        :param close:
        :param timeperiod:
        :return:
        """
        real = CCI(high, low, close, timeperiod=timeperiod)
        return real.reshape(len(high), 1)

    @staticmethod
    def chande_momentum_oscillator(close, timeperiod=14) -> ndarray:
        """
        名称：钱德动量摆动指标
        简介：与其他动量指标摆动指标如相对强弱指标（RSI）和随机指标（KDJ）不同，钱德动量指标在计算公式的分子中采用上涨日和下跌日的数据。

        指标应用
        1. 本指标类似RSI指标。
        2. 当本指标下穿-50水平时是买入信号，上穿+50水平是卖出信号。
        3. 钱德动量摆动指标的取值介于-100和100之间。
        4. 本指标也能给出良好的背离信号。
        5. 当股票价格创出新低而本指标未能创出新低时，出现牛市背离；
        6. 当股票价格创出新高而本指标未能创出新高时，出现熊市背离。
        7. 我们可以用移动均值对该指标进行平滑。


        :param close:
        :param timeperiod:
        :return:
        """
        real = CMO(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def directional_movement_index(
        high: ndarray, low: ndarray, close: ndarray, timeperiod=14
    ) -> ndarray:
        """
        名称：动向指标或趋向指标
        简介：通过分析股票价格在涨跌过程中买卖双方力量均衡点的变化情况，即多空双方的力量的变化受价格波动的影响而发生由均衡到失衡的循环过程，从而提供对趋势判断依据的一种技术指标。

        :param high:
        :param low:
        :param close:
        :param timeperiod:
        :return:
        """
        real = DX(high, low, close, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def moving_average_convergence_divergence(close: ndarray):
        """
        名称：平滑异同移动平均线
        简介：利用收盘价的短期（常用为12日）指数移动平均线与长期（常用为26日）指数移动平均线之间的聚合与分离状况，对买进、卖出时机作出研判的技术指标。

        :param close:
        :return:
        """
        macd, macd_signal, macd_hist = MACD(
            close, fastperiod=12, slowperiod=26, signalperiod=9
        )
        return np.concatenate([macd, macd_signal, macd_hist]).reshape(
            (len(close), 3), order="F"
        )

    @staticmethod
    def moving_average_convergence_divergence_extended(close: ndarray):
        """
        MACDEXT

        :param close:
        :return:
        """
        macd, macd_signal, macd_hist = MACDEXT(
            close,
            fastperiod=12,
            fastmatype=0,
            slowperiod=26,
            slowmatype=0,
            signalperiod=9,
            signalmatype=0,
        )
        return np.concatenate([macd, macd_signal, macd_hist]).reshape(
            (len(close), 3), order="F"
        )

    @staticmethod
    def money_flow_index(
        high: ndarray, low: ndarray, volume: ndarray, timeperiod=14
    ) -> ndarray:
        """
        名称：资金流量指标
        简介：属于量价类指标，反映市场的运行趋势

        :param high:
        :param low:
        :param volume:
        :param timeperiod:
        :return:
        """
        real = MFI(high, low, volume, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def minus_directional_indicator(
        high: ndarray, low: ndarray, close: ndarray, timeperiod=14
    ) -> ndarray:
        """
        名称：下升动向值
        简介：通过分析股票价格在涨跌过程中买卖双方力量均衡点的变化情况，即多空双方的力量的变化受价格波动的影响而发生由均衡到失衡的循环过程，从而提供对趋势判断依据的一种技术指标。

        :param high:
        :param low:
        :param close:
        :param timeperiod:
        :return:
        """
        real = MINUS_DI(high, low, close, timepeiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def minus_directional_movement(
        high: ndarray, low: ndarray, timeperiod=14
    ) -> ndarray:
        """
        名称：上升动向值 DMI中的DM代表正趋向变动值即上升动向值
        简介：通过分析股票价格在涨跌过程中买卖双方力量均衡点的变化情况，即多空双方的力量的变化受价格波动的影响而发生由均衡到失衡的循环过程，从而提供对趋势判断依据的一种技术指标。


        :param high:
        :param low:
        :param timeperiod:
        :return:
        """
        real = MINUS_DM(high, low, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def momentum(close: ndarray, timeperiod=10) -> ndarray:
        """
        名称： 上升动向值
        简介：投资学中意思为续航，指股票(或经济指数)持续增长的能力。研究发现，赢家组合在牛市中存在着正的动量效应，输家组合在熊市中存在着负的动量效应。

        :param close:
        :param timeperiod:
        :return:
        """
        real = MOM(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def plus_directional_indicator(
        high: ndarray, low: ndarray, close: ndarray, timeperiod=14
    ) -> ndarray:
        """
        PLUS_DI

        :param high:
        :param low:
        :param close:
        :param timeperiod:
        :return:
        """
        real = PLUS_DI(high, low, close, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def plus_directional_movement(
        high: ndarray, low: ndarray, timeperiod=14
    ) -> ndarray:
        """
        PLUS_DM

        :param high:
        :param low:
        :param timeperiod:
        :return:
        """
        real = PLUS_DM(high, low, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def percentage_price_oscillator(close: ndarray) -> ndarray:
        """
        名称：价格震荡百分比指数
        简介：价格震荡百分比指标（PPO）是一个和MACD指标非常接近的指标。
        PPO标准设定和MACD设定非常相似：12,26,9和PPO，和MACD一样说明了两条移动平均线的差距，但是它们有一个差别是PPO是用百分比说明。

        :param close:
        :return:
        """
        real = PPO(close, fastperiod=12, slowperiod=26, matype=0)
        return real.reshape((len(close), 1))

    @staticmethod
    def rate_of_change(close: ndarray, timeperiod=10) -> ndarray:
        """
        名称： 变动率指标
        简介：ROC是由当天的股价与一定的天数之前的某一天股价比较，其变动速度的大小,来反映股票市变动的快慢程度

        :param close:
        :param timeperiod:
        :return:
        """
        real = ROC(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def rate_of_change_percentage(close: ndarray, timeperiod=10) -> ndarray:
        """
        ROCP

        :param close:
        :param timeperiod:
        :return:
        """
        real = ROCP(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def rate_of_change_ratio(close: ndarray, timeperiod=10) -> ndarray:
        """
        ROCR

        :param close:
        :param timeperiod:
        :return:
        """
        real = ROCR(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def relative_strength_index(close: ndarray, timeperiod=14) -> ndarray:
        """
        名称：相对强弱指数
        简介：是通过比较一段时期内的平均收盘涨数和平均收盘跌数来分析市场买沽盘的意向和实力，从而作出未来市场的走势。

        :param close:
        :param timeperiod:
        :return:
        """
        real = RSI(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def stochastic(high: ndarray, low: ndarray, close: ndarray) -> ndarray:
        """
        名称：随机指标，俗称KD

        :param high:
        :param low:
        :param close:
        :return:
        """
        slowk, slowd = STOCH(
            high,
            low,
            close,
            fastk_period=5,
            slowk_period=3,
            slowk_matype=0,
            slowd_period=3,
            slowd_matype=0,
        )
        return np.concatenate([slowk, slowd]).reshape((len(high), 2), order="F")

    @staticmethod
    def stochastic_fast(high: ndarray, low: ndarray, close: ndarray) -> ndarray:
        """
        名称：随机指标fast

        :return:
        """
        fastk, fastd = STOCHF(
            high, low, close, fastk_period=5, fastd_period=3, fastd_matype=0
        )
        return np.concatenate([fastk, fastd]).reshape((len(high), 2), order="F")

    @staticmethod
    def stochastic_relative_strength_index(close: ndarray, timeperiod=14) -> ndarray:
        """


        :param close:
        :param timeperiod:
        :return:
        """
        fastk, fastd = STOCHRSI(
            close, timeperiod=timeperiod, fastk_period=5, fastd_period=3, fastd_matype=0
        )
        return np.concatenate([fastk, fastd]).reshape((len(close), 2), order="F")

    @staticmethod
    def rate_of_change_of_a_triple_smooth_ema(close: ndarray, timeperiod=30) -> ndarray:
        """


        :param close:
        :param timeperiod:
        :return:
        """
        real = TRIX(close, timeperiod=timeperiod)
        return real.reshape((len(close), 1))

    @staticmethod
    def ultimate_oscillator(high: ndarray, low: ndarray, close: ndarray) -> ndarray:
        """
        名称：终极波动指标
        简介：UOS是一种多方位功能的指标，除了趋势确认及超买超卖方面的作用之外，它的“突破”讯号不仅可以提供最适当的交易时机之外，更可以进一步加强指标的可靠度。

        :param high:
        :param low:
        :param close:
        :return:
        """
        real = ULTOSC(high, low, close, timeperiod1=7, timeperiod2=14, timeperiod3=28)
        return real.reshape((len(high), 1))

    @staticmethod
    def william_percentage_ratio(
        high: ndarray, low: ndarray, close: ndarray, timeperiod=14
    ) -> ndarray:
        """
        名称：威廉指标
        简介：WMS表示的是市场处于超买还是超卖状态。股票投资分析方法主要有如下三种：基本分析、技术分析、演化分析。在实际应用中，它们既相互联系，又有重要区别。

        :param high:
        :param low:
        :param close:
        :param timeperiod:
        :return:
        """
        real = WILLR(high, low, close, timeperiod=14)
        return real.reshape((len(high), 1))
