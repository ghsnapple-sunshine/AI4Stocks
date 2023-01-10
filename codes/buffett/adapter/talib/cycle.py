import numpy as np
from talib import HT_DCPERIOD, HT_DCPHASE, HT_PHASOR, HT_SINE, HT_TRENDMODE

from buffett.adapter.numpy import ndarray


class Cycle:
    @staticmethod
    def dominant_cycle_period(close: ndarray) -> ndarray:
        """
        名称：希尔伯特变换-主导周期
        简介：将价格作为信息信号，计算价格处在的周期的位置，作为择时的依据。

        :param close:
        :return:
        """
        real = HT_DCPERIOD(close)
        return real.reshape((len(close), 1))

    @staticmethod
    def dominant_cycle_phase(close: ndarray) -> ndarray:
        """
        名称：希尔伯特变换-主导循环阶段

        :param close:
        :return:
        """
        real = HT_DCPHASE(close)
        return real.reshape((len(close), 1))

    @staticmethod
    def phasor_components(close: ndarray) -> ndarray:
        """
        名称：希尔伯特变换-希尔伯特变换相量分量

        :param close:
        :return:
        """
        inphase, quadrature = HT_PHASOR(close)
        return np.concatenate([inphase, quadrature]).reshape((len(close), 2), order="F")

    @staticmethod
    def sinewave(close: ndarray) -> ndarray:
        """
        名称： 希尔伯特变换-正弦波

        :param close:
        :return:
        """
        sine, leadsine = HT_SINE(close)
        return np.concatenate([sine, leadsine]).reshape((len(close), 2), order="F")

    @staticmethod
    def trend_vs_cycle_mode(close: ndarray) -> ndarray:
        """
        名称： 希尔伯特变换-趋势与周期模式

        :param close:
        :return:
        """
        integer = HT_TRENDMODE(close)
        return integer.reshape((len(close), 1))
