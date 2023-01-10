from talib import ATR, NATR, TRANGE

from buffett.adapter.numpy import ndarray


class Volatility:
    @staticmethod
    def average_true_range(high: ndarray, low: ndarray, close: ndarray, timeperiod=14):
        """
        名称：真实波动幅度均值
        简介：真实波动幅度均值（ATR)是 以 N 天的指数移动平均数平均後的交易波动幅度。

        特性：
        波动幅度的概念表示可以显示出交易者的期望和热情。
        大幅的或增加中的波动幅度表示交易者在当天可能准备持续买进或卖出股票。
        波动幅度的减少则表示交易者对股市没有太大的兴趣。

        :param high:
        :param low:
        :param close:
        :param timeperiod:
        :return:
        """
        real = ATR(high, low, close, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def normalized_average_true_range(
        high: ndarray, low: ndarray, close: ndarray, timeperiod=14
    ):
        """
        名称：归一化波动幅度均值

        :param high:
        :param low:
        :param close:
        :param timeperiod:
        :return:
        """
        real = NATR(high, low, close, timeperiod=timeperiod)
        return real.reshape((len(high), 1))

    @staticmethod
    def true_range(high: ndarray, low: ndarray, close: ndarray) -> ndarray:
        """
        TRANGE

        :param high:
        :param low:
        :param close:
        :return:
        """
        real = TRANGE(high, low, close)
        return real.reshape((len(high), 1))
