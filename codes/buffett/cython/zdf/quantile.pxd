from tree cimport Tree


cdef class Quantile:
    cdef int _stat_period, _d0, _d1
    cdef double _w0, _w1, _w

    # cdef double get_value(self, Tree tree)
    cpdef double get_value(self, Tree tree)