from datetime import date
from datetime import datetime
from typing import Union, Optional

from buffett.common.error import ParamTypeError
from buffett.common.pendelum import Date, DateTime




'''
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
'''


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




def dt2d(dt: DateTime) -> Date:
    """
    DateTime转换为Date

    :param dt:          待转换的DateTime
    :return:            转换后的Date
    """
    return Date(dt.year, dt.month, dt.day)
