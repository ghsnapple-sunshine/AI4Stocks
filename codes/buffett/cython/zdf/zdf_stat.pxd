cimport numpy as c_np


cpdef c_np.ndarray[float, ndim=2] stat_past_with_period(c_np.ndarray[float, ndim=2] arr, int period)