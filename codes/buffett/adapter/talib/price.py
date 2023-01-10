from talib import AVGPRICE, MEDPRICE, TYPPRICE, WCLPRICE

from buffett.adapter.numpy import ndarray


class Price:
    @staticmethod
    def average_price(
        open: ndarray, high: ndarray, low: ndarray, close: ndarray
    ) -> ndarray:
        """
        名称：平均价格函数

        :param open:
        :param high:
        :param low:
        :param close:
        :return:
        """
        real = AVGPRICE(open, high, low, close)
        return real.reshape((len(open), 1))

    @staticmethod
    def median_price(high: ndarray, low: ndarray) -> ndarray:
        """
        名称：中位数价格

        :param high:
        :param low:
        :return:
        """
        real = MEDPRICE(high, low)
        return real.reshape((len(high), 1))

    @staticmethod
    def typical_price(high: ndarray, low: ndarray, close: ndarray) -> ndarray:
        """
        代表性价格

        :param high:
        :param low:
        :param close:
        :return:
        """
        real = TYPPRICE(high, low, close)
        return real.reshape((len(high), 1))

    @staticmethod
    def weighted_close_price(high: ndarray, low: ndarray, close: ndarray) -> ndarray:
        """
        加权收盘价

        :param high:
        :param low:
        :param close:
        :return:
        """
        real = WCLPRICE(high, low, close)
        return real.reshape((len(high), 1))
