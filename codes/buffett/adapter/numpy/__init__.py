from numpy import NAN as np_NAN
from numpy import arange as np_arange
from numpy import array as np_array
from numpy import multiply as np_multiply
from numpy import ndarray as np_ndarray
from numpy import reshape as np_reshape
from numpy import sum as np_sum
from numpy import zeros as np_zeros

# Constants
NAN = np_NAN

# Types
ndarray = np_ndarray


class np:
    # methods
    arange = np_arange
    array = np_array
    ndarray = np_ndarray
    multiply = np_multiply
    sum = np_sum
    reshape = np_reshape
    zeros = np_zeros
