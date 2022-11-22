from numpy import (
    NAN as np_NAN,
    arange as np_arange,
    array as np_array,
    float64 as np_float64,
    linspace as np_linspace,
    multiply as np_multiply,
    ndarray as np_ndarray,
    reshape as np_reshape,
    sum as np_sum,
    zeros as np_zeros,
)

# Constants
NAN = np_NAN

# Types
ndarray = np_ndarray
float64 = np_float64


class np:
    # methods
    arange = np_arange
    array = np_array
    linspace = np_linspace
    ndarray = np_ndarray
    multiply = np_multiply
    sum = np_sum
    reshape = np_reshape
    zeros = np_zeros
