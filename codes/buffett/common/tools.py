from buffett.adapter.numpy import np
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col.meta import META_COLS
from buffett.common.pendulum import DateTime, Duration, DateSpan


def tuple_to_array(tup: tuple) -> np.array:
    arr = np.array(tup)
    arr = np.reshape(arr, (-1, len(tup)))
    return arr


def get_now_shift(du: Duration, minus=False) -> DateTime:
    now = DateTime.now()
    if minus:
        return now - du
    return now + du


def create_meta(meta_list: list) -> DataFrame:
    """

    :rtype: object
    """
    return DataFrame(data=meta_list, columns=META_COLS)


def dataframe_is_valid(df: DataFrame) -> bool:
    return isinstance(df, DataFrame) and not (df.empty or df.index.empty)


def dataframe_not_valid(df: DataFrame) -> bool:
    return not isinstance(df, DataFrame) or (df.empty and df.index.empty)


def span_is_valid(span: DateSpan) -> bool:
    return isinstance(span, DateSpan)


def span_not_valid(span: DateSpan) -> bool:
    return not isinstance(span, DateSpan)


def list_not_valid(ls: list) -> bool:
    return not (isinstance(ls, list) and len(ls) > 0)


def list_is_valid(ls: list) -> bool:
    return isinstance(ls, list) and len(ls) > 0
