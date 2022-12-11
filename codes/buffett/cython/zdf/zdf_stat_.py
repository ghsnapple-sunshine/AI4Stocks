from buffett.adapter.numpy import ndarray
from buffett.cython.zdf.zdf_stat import stat_past_with_period as cy_stat_past_with_period


def stat_past_with_period(arr: ndarray, period: int):
    """
    wrapper of a cython method 'stat_past_with_period'

    :param arr:
    :param period:
    :return:
    """
    return cy_stat_past_with_period(arr, period)
