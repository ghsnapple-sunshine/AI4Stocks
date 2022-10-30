from datetime import date, datetime

from pandas import Timestamp

from buffett.common.pendelum import Date, DateTime


def to_my_datetime(dt):
    """
    将datetime进行封装

    :param dt:
    :return:
    """
    if isinstance(dt, datetime):
        return DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)
    else:
        return


def to_my_date(dt):
    """
    将date进行封装

    :param dt:
    :return:
    """
    if isinstance(dt, date):
        return Date(dt.year, dt.month, dt.day)
    else:
        return


def date_to_datetime(dt: Date) -> DateTime:
    """
    Date转换成DateTime

    :param dt:          待转换的Date
    :return:            转换后的DateTime
    """
    return DateTime(dt.year, dt.month, dt.day)


def datetime_to_date(dt: DateTime) -> Date:
    """
    DateTime转换为Date

    :param dt:          待转换的DateTime
    :return:            转换后的Date
    """
    return Date(dt.year, dt.month, dt.day)


def timestamp_to_datetime(timestamp: Timestamp) -> DateTime:
    dt = DateTime(
        year=timestamp.year,
        month=timestamp.month,
        day=timestamp.day,
        hour=timestamp.hour,
        minute=timestamp.minute,
        second=timestamp.second,
        microsecond=timestamp.microsecond,
        tzinfo=timestamp.tzinfo)
    return dt
