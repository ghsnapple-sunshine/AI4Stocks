import numpy as np

from tree cimport Tree
from quantile cimport Quantile


# cpdef c_np.ndarray[double, ndim=2] stat_past_with_period(c_np.ndarray[double, ndim=2] arr, int period):
def stat_past_with_period(arr, period):
    # S0
    # print("Enter 'stat_past_with_period'.")
    # S1
    cdef int CLOSEd = 0
    cdef int HIGHd = 1
    cdef int LOWd = 2
    # print("Finish create CLOSEd, HIGHd, LOWd.")
    # S2
    cdef int num = len(arr)
    # print("Finish create num.")
    res = np.zeros([num, 8], dtype=float)
    # cdef c_np.ndarray[double, ndim=2] res = c_np.PyArray_ZEROS(nd=2, dims=[num, 8], type=c_np.NPY_FLOAT, fortran=0)
    # print("Finish create res.")
    # S3
    cdef Tree closes = Tree()
    cdef Tree highs = Tree()
    cdef Tree lows = Tree()
    # print("Finish create closes, highs and lows.")
    # S4
    cdef list quans = [Quantile(x, period) for x in [1, 5, 10, 90, 95, 99]]
    # print("Finish create closes, highs and lows.")
    # S5
    cdef int i
    for i in range(0, period):
        closes.add(arr[i, CLOSEd])
        highs.add(arr[i, HIGHd])
        lows.add(arr[i, LOWd])
    # print("Finish warm-up stages.")
    # S6
    for i in range(period, num):
        # S6.1
        # print(f"--round:{i}--")
        # print([closes.get_nth(x) for x in range(0, 100)])
        # S6.2 计算
        res[i, 0:6] = [q.get_value(closes) for q in quans]  # 1%, 5%, 10%, 90%, 95%, 99%
        res[i, 6] = lows.get_nth(-1)  # 最高
        res[i, 7] = highs.get_nth(0)  # 最低
        # S6.3 更新值
        closes.delete(arr[i - period, CLOSEd])
        closes.add(arr[i, CLOSEd])
        highs.delete(arr[i - period, HIGHd])
        highs.add(arr[i, HIGHd])
        lows.delete(arr[i - period, LOWd])
        lows.add(arr[i, LOWd])
    # print("Finish all calculates. Exit 'stat_past_with_period'.")
    # S7
    return res
