from libc.math cimport floor


cdef class Quantile:
    def __init__(self, pct: int, period: int):
        if pct < 0 or pct > 100:
            raise ValueError("'pct' should be in [0, 100]")
        if period < 1:
            raise ValueError("'period' should be in (0, )")
        self._stat_period = period
        cdef double q = (period - 1) / 100
        self._d0 = <int>(floor(pct * q))
        self._d1 = self._d0 + 1
        cdef double p0 = self._d0 / q
        cdef double p1 = self._d1 / q
        self._w0 = p1 - pct
        self._w1 = pct - p0
        self._w = p1 - p0

    # cdef double get_value(self, Tree tree):
    cpdef double get_value(self, Tree tree):
        """
        获取n百分位的值

        :param tree:
        :return:
        """
        cdef double v0 = tree.get_nth(self._d0)
        cdef double v1 = tree.get_nth(self._d1)
        return (v0 * self._w0 + v1 * self._w1) / self._w