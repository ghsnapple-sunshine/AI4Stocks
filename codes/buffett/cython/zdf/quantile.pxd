from tree cimport Tree


cdef class Quantile:
    cdef int _stat_period, _d0, _d1
    cdef float _w0, _w1, _w

    cdef float get_value(self, Tree tree)