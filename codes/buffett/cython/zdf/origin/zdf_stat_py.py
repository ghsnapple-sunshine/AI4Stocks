from buffett.adapter.numpy import ndarray, np
from buffett.cython.zdf.origin.quantile_py import Quantile
from buffett.cython.zdf.origin.tree_py import Tree

CLOSEd, HIGHd, LOWd = 0, 1, 2


def stat_past_with_period(arr: ndarray, period: int) -> ndarray:
    num = len(arr)
    res = np.zeros([num, 8], dtype=float)
    closes, highs, lows = Tree(), Tree(), Tree()

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
