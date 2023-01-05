from buffett.adapter.numpy import ndarray, np
from buffett.cython.zdf.origin.quantile_py import Quantile
from buffett.cython.zdf.origin.tree_py import Tree

CLOSEd, HIGHd, LOWd = 0, 1, 2


def stat_future_period(arr: ndarray, period: int) -> ndarray:
    num = len(arr)
    res = np.zeros([num, 8], dtype=float)
    closes, highs, lows = Tree(), Tree(), Tree()

    quans = [Quantile(x, period) for x in [1, 5, 10, 90, 95, 99]]
    for i in range(1, period):  # 插入d[t+1: t+n)
        closes.add(arr[i, CLOSEd])
        highs.add(arr[i, HIGHd])
        lows.add(arr[i, LOWd])
    for i in range(num - period):
        # 插入d[t+n]
        closes.add(arr[i + period, CLOSEd])
        highs.add(arr[i + period, HIGHd])
        lows.add(arr[i + period, LOWd])
        # 计算
        res[i, 0:6] = [q.get_value(closes) for q in quans]  # 1%, 5%, 10%, 90%, 95%, 99%
        res[i, 6] = lows.get_nth(-1)  # 最高
        res[i, 7] = highs.get_nth(0)  # 最低
        # 移除d[t+1]
        closes.delete(arr[i + 1, CLOSEd])
        highs.delete(arr[i + 1, HIGHd])
        lows.delete(arr[i + 1, LOWd])

    res = (res / arr[:, CLOSEd].reshape((num, 1)) - 1) * 100
    return res
