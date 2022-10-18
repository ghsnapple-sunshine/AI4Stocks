import numpy as np
from pandas import Timestamp
from pendulum import DateTime, Duration


def tuple_to_array(tup: tuple) -> np.array:
    arr = np.array(tup)
    arr = np.reshape(arr, (-1, len(tup)))
    return arr


def timestamp_to_datetime(ts: Timestamp) -> DateTime:
    dt = DateTime(
        year=ts.year,
        month=ts.month,
        day=ts.day,
        hour=ts.hour,
        minute=ts.minute,
        second=ts.second,
        microsecond=ts.microsecond,
        tzinfo=ts.tzinfo)
    return dt


def get_now_shift(
        du: Duration,
        minus=False
) -> DateTime:
    now = DateTime.now()
    if minus:
        return now - du
    return now + du

