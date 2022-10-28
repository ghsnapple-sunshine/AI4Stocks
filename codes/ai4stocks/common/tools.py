import numpy as np
from pandas import Timestamp, DataFrame

from ai4stocks.common.pendelum import DateTime, Duration
from ai4stocks.constants.meta import META_COLS


def tuple_to_array(tup: tuple) -> np.array:
    arr = np.array(tup)
    arr = np.reshape(arr, (-1, len(tup)))
    return arr




def get_now_shift(du: Duration, minus=False) -> DateTime:
    now = DateTime.now()
    if minus:
        return now - du
    return now + du


def datetime_to_date(datetime: DateTime) -> DateTime:
    return DateTime(
        year=datetime.year,
        month=datetime.month,
        day=datetime.day
    )


def create_meta(meta_list: list) -> DataFrame:
    return DataFrame(data=meta_list, columns=META_COLS)
