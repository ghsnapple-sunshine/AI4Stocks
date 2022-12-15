from typing import Optional, Union

from buffett.adapter.numpy import np
from buffett.adapter.pendulum import date, datetime
from buffett.common.error import ParamTypeError
from buffett.common.pendulum.date import Date
from buffett.common.pendulum.datetime import DateTime


def convert_date(dt: Optional[date]) -> Union[Date, DateTime, None]:
    """
    将可支持的格式（如原生date, datetime, pd.date, pd.TimeStamp等）转换成自有Date/DateTime

    :param dt:
    :return:
    """
    if isinstance(dt, datetime):
        return DateTime(
            year=dt.year,
            month=dt.month,
            day=dt.day,
            hour=dt.hour,
            minute=dt.minute,
            second=dt.second,
            microsecond=dt.microsecond,
        )
    elif isinstance(dt, date):
        return Date(dt.year, dt.month, dt.day)
    elif dt is None:
        return None
    raise ParamTypeError("dt", Optional[date])


convert_dates = np.vectorize(convert_date)


def convert_datetime(dt: Optional[date]) -> Union[Date, DateTime, None]:
    """
    将可支持的格式（如原生date, datetime, pd.date, pd.TimeStamp等）转换成自有DateTime

    :param dt:
    :return:
    """
    if isinstance(dt, datetime):
        return DateTime(
            year=dt.year,
            month=dt.month,
            day=dt.day,
            hour=dt.hour,
            minute=dt.minute,
            second=dt.second,
            microsecond=dt.microsecond,
        )
    elif isinstance(dt, date):
        return DateTime(dt.year, dt.month, dt.day)
    elif dt is None:
        return None
    raise ParamTypeError("dt", Optional[date])


convert_datetimes = np.vectorize(convert_datetime)
