from datetime import date, datetime

from ai4stocks.common.pendelum import Date, DateTime


def to_my_datetime(dt):
    """
    将datetime进行封装

    :param dt:
    :return:
    """
    if isinstance(dt, datetime):
        return DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
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
