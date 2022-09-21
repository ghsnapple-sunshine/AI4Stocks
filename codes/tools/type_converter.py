from itertools import chain
import numpy as np


class TypeConverter:
    @staticmethod
    def Tuple2List(tup: tuple):
        ls = list(chain.from_iterable(tup))
        return np.reshape(ls, (len(tup), len(ls) // len(tup)))
