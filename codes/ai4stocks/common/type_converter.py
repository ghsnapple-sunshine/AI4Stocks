from itertools import chain
import numpy as np
from pandas import Timestamp
from pendulum import DateTime


class TypeConverter:
    @staticmethod
    def Tuple2Arr(
            tup: tuple
    ) -> np.array:
        arr = np.array(tup)
        arr = np.reshape(arr, (-1, len(tup)))
        return arr

    @staticmethod
    def Timestamp2Datetime(
            ts: Timestamp
    ) -> DateTime:
        dt = DateTime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second, ts.microsecond, ts.tzinfo)
        return dt
