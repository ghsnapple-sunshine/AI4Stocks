import numpy as np

from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import CLOSE, HIGH, LOW, DATETIME
from buffett.common.constants.col.analysis import (
    DF5_PCT99,
    DF5_PCT95,
    DF5_PCT90,
    ZF5_PCT90,
    ZF5_PCT95,
    ZF5_PCT99,
    DF5_MAX,
    ZF5_MAX,
    DF20_PCT99,
    DF20_PCT95,
    DF20_PCT90,
    ZF20_PCT90,
    ZF20_PCT95,
    ZF20_PCT99,
    DF20_MAX,
    ZF20_MAX,
)
from buffett.cython.zdf.zdf_stat_ import stat_past_with_period

CLOSEd, HIGHd, LOWd = 0, 1, 2


def calculate_zdf(df: DataFrame) -> DataFrame:
    arr = df[[CLOSE, HIGH, LOW]].values
    stat5 = stat_past_with_period(arr, period=5)
    stat20 = stat_past_with_period(arr, period=20)
    stat = np.concatenate([stat5, stat20], axis=1)
    columns = [
        DF5_PCT99,
        DF5_PCT95,
        DF5_PCT90,
        ZF5_PCT90,
        ZF5_PCT95,
        ZF5_PCT99,
        DF5_MAX,
        ZF5_MAX,
        DF20_PCT99,
        DF20_PCT95,
        DF20_PCT90,
        ZF20_PCT90,
        ZF20_PCT95,
        ZF20_PCT99,
        DF20_MAX,
        ZF20_MAX,
    ]
    data = DataFrame(data=stat, columns=columns)
    data[DATETIME] = df.index
    return data


'''
def stat_past_with_period(arr: ndarray, period: int) -> ndarray:
    num = len(arr)
    res = np.zeros([num, 8], dtype=float)
    closes, highs, lows = SizeBalancedTree(), SizeBalancedTree(), SizeBalancedTree()

    quans = [Quantile(x, period) for x in [1, 5, 10, 90, 95, 99]]
    for i in range(0, period):
        closes.add(arr[i, CLOSEd])
        highs.add(arr[i, HIGHd])
        lows.add(arr[i, LOWd])
    for i in range(period, num):
        # 计算
        res[i, 0:6] = [
            q.get_value(closes) for q in quans
        ]  # 1%, 5%, 10%, 90%, 95%, 99%
        res[i, 6] = lows.get_nth(-1)  # 最高
        res[i, 7] = highs.get_nth(0)  # 最低
        # 更新值
        closes.delete(arr[i - period, CLOSEd])
        closes.add(arr[i, CLOSEd])
        highs.delete(arr[i - period, HIGHd])
        highs.add(arr[i, HIGHd])
        lows.delete(arr[i - period, LOWd])
        lows.add(arr[i, LOWd])
    return res


class Quantile:
    def __init__(self, pct: int, period: int):
        if pct < 0 or pct > 100:
            raise ValueError("'pct' should be in [0, 100]")
        if period < 1:
            raise ValueError("'period' should be in (0, )")
        self._stat_period = period
        q = (period - 1) / 100
        self._d0 = math.floor(pct * q)
        self._d1 = self._d0 + 1
        p0 = self._d0 / q
        p1 = self._d1 / q
        self._w0 = p1 - pct
        self._w1 = pct - p0
        self._w = p1 - p0

    @property
    def d0(self) -> int:
        return self._d0

    @property
    def d1(self) -> int:
        return self._d1

    def get_value(self, tree: SizeBalancedTree) -> float:
        """
        获取n百分位的值

        :param tree:
        :return:
        """
        v0 = tree.get_nth(self._d0)
        v1 = tree.get_nth(self._d1)
        return (v0 * self._w0 + v1 * self._w1) / self._w
'''