from numpy import (
    NAN as np_NAN,
    abs as np_abs,
    arange as np_arange,
    array as np_array,
    concatenate as np_concatenate,
    datetime64 as np_datetime64,
    float64 as np_float64,
    linspace as np_linspace,
    max as np_max,
    min as np_min,
    multiply as np_multiply,
    ndarray as np_ndarray,
    percentile as np_percentile,
    random as np_random,
    reshape as np_reshape,
    sum as np_sum,
    vectorize as np_vectorize,
    unique as np_unique,
    zeros as np_zeros,
)

# Constants
NAN = np_NAN

# Types
ndarray = np_ndarray
float64 = np_float64
datetime64 = np_datetime64


class np:
    maxs = np_vectorize(max)
    mins = np_vectorize(min)
    # methods
    abs = np_abs
    arange = np_arange
    array = np_array
    concatenate = np_concatenate
    linspace = np_linspace
    ndarray = np_ndarray
    max = np_max
    min = np_min
    multiply = np_multiply
    sum = np_sum
    percentile = np_percentile
    random = np_random
    reshape = np_reshape
    vectorize = np_vectorize
    unique = np_unique
    zeros = np_zeros
