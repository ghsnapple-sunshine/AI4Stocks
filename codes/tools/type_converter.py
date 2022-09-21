from itertools import chain
import numpy as np


class TypeConverter:
    @staticmethod
    def Tuple2Arr(tup: tuple):
        arr = np.array(tup)
        arr = np.reshape(arr, (-1, len(tup)))
        return arr
