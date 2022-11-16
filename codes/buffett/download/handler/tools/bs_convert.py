from typing import Optional


from buffett.adapter.baostock import ResultData
from buffett.adapter.pandas import DataFrame
from buffett.common.pendelum import DateTime
from buffett.common.constants import INT_MAX_VALUE, INT_MIN_VALUE, FLOAT_MAX_VALUE, FLOAT_MIN_VALUE


def bs_result_to_dataframe(rs: ResultData) -> DataFrame:
    """
    将baostock返回的ResultData转换为DataFrame

    :param rs:      ResultData
    :return:        DataFrame
    """
    data = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data.append(rs.get_row_data())
    return DataFrame(data, columns=rs.fields)


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
    _float = float(str_float)
    if (_float > FLOAT_MAX_VALUE) | (_float < FLOAT_MIN_VALUE):
        return None
    return str_float
