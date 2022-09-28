import numpy as np
from pandas import Timestamp
from pendulum import DateTime, Duration


def Tuple2Arr(tup: tuple) -> np.array:
    arr = np.array(tup)
    arr = np.reshape(arr, (-1, len(tup)))
    return arr


def Timestamp2Datetime(ts: Timestamp) -> DateTime:
    dt = DateTime(ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second, ts.microsecond, ts.tzinfo)
    return dt


def GetNowShift(
        du: Duration,
        minus=False
) -> DateTime:
    now = DateTime.now()
    if minus:
        return now - du
    return now + du
