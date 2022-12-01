from math import sqrt

from buffett.adapter.numpy import ndarray
from buffett.adapter.pandas import DataFrame, pd, Series
from buffett.adapter.pendulum import Date
from buffett.adapter.statsmodel import sm
from buffett.common.constants.col import CLOSE
from buffett.common.pendulum import convert_date

BFQ_SUFFIX, HFQ_SUFFIX = "_bfq", "_hfq"
CLOSE_BFQ, CLOSE_HFQ = CLOSE + BFQ_SUFFIX, CLOSE + HFQ_SUFFIX
EPS = 1e-2


def reform(data: Series, factor: list[list], name: str):
    conv = _Converter(factor=factor)
    return Series(
        data=[conv.calc(k, data[k]) for k in data.keys()],
        index=data.index,
        name=name,
    )


def get_fuquan_factor(bfq_data: Series, hfq_data: Series):
    merge_data = pd.merge(
        bfq_data,
        hfq_data,
        left_index=True,
        right_index=True,
        suffixes=[BFQ_SUFFIX, HFQ_SUFFIX],
    )
    groups = _group_data(merge_data.values)
    calendar_data = merge_data.index.to_list()
    reg = _linear_regression(merge_data, calendar_data, groups)
    return reg


def _group_data(stk_data: ndarray) -> list[list[int]]:
    """
    将满足三点共线的点进行分组
    （两次复权间的数据满足三点共线）

    :param stk_data:
    :return:
    """
    ls = []
    # 头处理
    if _collinear(stk_data[0:3]):
        pair = [0, 1]
    else:
        if _collinear(stk_data[1:4]):
            ls.append([0, 0])
            pair = [1, 1]
        else:
            ls.append([0, 1])
            pair = [2, 2]
    # 整体处理
    start, end = pair[1], len(stk_data) - 2
    i = start
    while i < end:
        if _collinear(stk_data[i : i + 3]):
            i += 1
            continue
        pair[1] = i + 1
        ls.append(pair)
        pair = [i + 2, i + 2]
        i += 2
    # 尾处理
    pair[1] = len(stk_data) - 1
    if pair[1] >= pair[0]:
        ls.append(pair)
    return ls


def _linear_regression(
    stock_data: DataFrame, calendar_data: list[Date], groups: list[list[int]]
):
    ls = []
    for g in groups:
        fit0 = sm.formula.ols(
            f"{CLOSE_BFQ}~{CLOSE_HFQ}", data=stock_data[g[0] : g[1] + 1]
        ).fit()
        b0, a0 = fit0.params  # y = b + ax
        b1, a1 = -b0 / a0, 1 / a0  # x = (-b/a) + (1/a)y
        ls.append(
            [
                convert_date(calendar_data[g[0]]),
                convert_date(calendar_data[g[1]]).add(days=1),
                b0,
                a0,
                b1,
                a1,
            ]
        )
    return ls


def _collinear(stock_data: ndarray) -> bool:
    """
    计算三点共线

    :param stock_data:
    :return:
    """
    """|(y3−y1)(x2−x1)−(y2−y1)(x3−x1)|<=EPS"""
    """
    return (stock_data[2, 1] - stock_data[0, 1]) * (
        stock_data[1, 0] - stock_data[0, 0]
    ) - (stock_data[1, 1] - stock_data[0, 1]) * (
        stock_data[2, 0] - stock_data[0, 0]
    ) < EPS
    """
    x1, y1 = stock_data[0, 0], stock_data[0, 1]
    x2, y2 = stock_data[1, 0], stock_data[1, 1]
    x3, y3 = stock_data[2, 0], stock_data[2, 1]
    vx1, vy1 = x2 - x1, y2 - y1
    vx2, vy2 = x3 - x1, y3 - y1
    return (
        abs(vy2 * vx1 - vy1 * vx2) / sqrt((vx1**2 + vy1**2) * (vx2**2 + vy2**2))
        < EPS
    )


class _Converter:
    def __init__(self, factor: list[list]):
        self._matrix = factor
        self._used = 0
        row = factor[self._used]
        self._end = row[1]
        self._b = row[2]
        self._a = row[3]

    def calc(self, date, inpt) -> float:
        if date >= self._end:
            self._used += 1
            row = self._matrix[self._used]
            self._end = row[1]
            self._b = row[2]
            self._a = row[3]
        return self._b + self._a * inpt
