from typing import Optional

from buffett.common.constants import (
    INT_MAX_VALUE,
    INT_MIN_VALUE,
    FLOAT_MAX_VALUE,
    FLOAT_MIN_VALUE,
)
from buffett.common.pendulum import DateTime


def bs_str_to_datetime(str_datetime: str) -> DateTime:
    """
    将baostock返回的str类型的datetime转换成DateTime

    :param str_datetime:    str类型的DateTime
    :return:                DateTime
    """
    year = int(str_datetime[0:4])
    month = int(str_datetime[4:6])
    day = int(str_datetime[6:8])
    hour = int(str_datetime[8:10])
    minute = int(str_datetime[10:12])
    return DateTime(year=year, month=month, day=day, hour=hour, minute=minute)


def bs_check_int(str_int: str) -> Optional[str]:
    """
    检查baostock返回的str类型的int的有效性
    (如果有效返回自身，如果无效返回None）

    :param str_int:         str类型的int
    :return:                检查结果
    """
    if str_int == "":
        return None
    _int = int(str_int)
    if (_int > INT_MAX_VALUE) | (_int < INT_MIN_VALUE):
        return None
    return str_int


def bs_check_float(str_float: str) -> Optional[str]:
    """
    检查baostock返回的str类型的float的有效性
    (如果有效返回自身，如果无效返回None）

    :param str_float:       str类型的float
    :return:                检查结果
    """
    if str_float == "":
        return None
    _float = float(str_float)
    if (_float > FLOAT_MAX_VALUE) | (_float < FLOAT_MIN_VALUE):
        return None
    return str_float


def bs_convert_code(code: str) -> str:
    """
    把股票代码转换成baostock需要的格式

    :param code:
    :return:
    """
    if code[0] == "6":
        return "sh." + code
    elif code[0] == "0":
        return "sz." + code
    elif code[0] == "3":
        return "sz." + code
    raise NotImplemented
