from libc.math cimport floor


cdef class Quantile:
    def __init__(self, pct: int, period: int):
        if pct < 0 or pct > 100:
            raise ValueError("'pct' should be in [0, 100]")
        if period < 1:
            raise ValueError("'period' should be in (0, )")
        self._stat_period = period
        cdef float q = (period - 1) / 100
        self._d0 = <int>(floor(pct * q))
        self._d1 = self._d0 + 1
        cdef float p0 = self._d0 / q
        cdef float p1 = self._d1 / q
        self._w0 = p1 - pct
        self._w1 = pct - p0
        self._w = p1 - p0

    cdef float get_value(self, Tree tree):
        """
        获取n百分位的值

        :param tree:
        :return:
        """
        cdef float v0 = tree.get_nth(self._d0)
        cdef float v1 = tree.get_nth(self._d1)
        return (v0 * self._w0 + v1 * self._w1) / self._w