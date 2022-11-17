from typing import Optional, Union

from buffett.adapter.pendulum import date, datetime
from buffett.common.error import ParamTypeError
from buffett.common.pendelum.date import Date
from buffett.common.pendelum.datetime import DateTime


def convert_date(dt: Optional[date]) -> Union[Date, DateTime, None]:
    """
    将可支持的格式（如原生date, datetime, pd.date, pd.TimeStamp等）转换成自有Date/DateTime

    :param dt:
    :return:
    """
    if isinstance(dt, datetime):
        return DateTime(
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            microsecond=dt.microsecond,
            tzinfo=dt.tzinfo,
        )
    elif isinstance(dt, date):
        return Date(dt.year, dt.month, dt.day)
    elif dt is None:
        return None
    raise ParamTypeError("dt", Union[date, None])


def convert_datetime(dt: Optional[date]) -> Union[Date, DateTime, None]:
    """
    将可支持的格式（如原生date, datetime, pd.date, pd.TimeStamp等）转换成自有DateTime

    :param dt:
    :return:
    """
    if isinstance(dt, datetime):
        return DateTime(
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            microsecond=dt.microsecond,
            tzinfo=dt.tzinfo,
            fold=dt.fold,
        )
    elif isinstance(dt, date):
        return DateTime(dt.year, dt.month, dt.day)
    elif dt is None:
        return None
    raise ParamTypeError("dt", Union[date, None])
