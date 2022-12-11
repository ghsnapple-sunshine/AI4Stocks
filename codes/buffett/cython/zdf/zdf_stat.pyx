from tree cimport Tree
from quantile cimport Quantile


cpdef c_np.ndarray[float, ndim=2] stat_past_with_period(c_np.ndarray[float, ndim=2] arr, int period):
    cdef int CLOSEd = 0
    cdef int HIGHd = 1
    cdef int LOWd = 2

    cdef int num = len(arr)
    cdef c_np.ndarray[float, ndim=2] res = c_np.PyArray_ZEROS(nd=2, dims=[num, 8], type=c_np.NPY_FLOAT, fortran=0)
    cdef Tree closes = Tree()
    cdef Tree highs = Tree()
    cdef Tree lows = Tree()

    cdef list quans = [Quantile(x, period) for x in [1, 5, 10, 90, 95, 99]]
    cdef int i
    for i in range(0, period):
        closes.add(arr[i, CLOSEd])
        highs.add(arr[i, HIGHd])
        lows.add(arr[i, LOWd])
    for i in range(period, num):
        # 计算
        res[i, 0:6] = [q.get_value(closes) for q in quans]  # 1%, 5%, 10%, 90%, 95%, 99%
        res[i, 6] = lows.get_nth(-1)  # 最高
        res[i, 7] = highs.get_nth(0)  # 最低
        # 更新值
        closes.delete(arr[i - period, CLOSEd])
        closes.add(arr[i, CLOSEd])
        highs.delete(arr[i - period, HIGHd])
        highs.add(arr[i, HIGHd])
        lows.delete(arr[i - period, LOWd])
        lows.add(arr[i, LOWd])
    return res
