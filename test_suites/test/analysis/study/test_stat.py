from zdf_stat import stat_past_with_period

from buffett.adapter.numpy import np, ndarray
from buffett.adapter.pandas import DataFrame
from buffett.cython.zdf.origin import stat_past_with_period as stat_past_with_period_py
from test import SimpleTester


class TestStatisticsAnalyst(SimpleTester):
    @classmethod
    def _setup_once(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_with_excepetaion(self):
        inputs1 = np.zeros(shape=(500, 3), dtype=float)
        inputs1[:, 0] = np.arange(0, 500, dtype=float)
        inputs1[:, 1] = np.arange(0, 500, dtype=float)
        inputs1[:, 2] = np.arange(0, 500, dtype=float)

        res1 = stat_past_with_period(arr=inputs1, period=100)
        exception = np.zeros(shape=(500, 8), dtype=float)
        q = [1, 5, 10, 90, 95, 99]
        for i in range(100, 500):
            exception[i, 0:6] = [(x * 0.99 + i - 100) for x in q]
            exception[i, 6] = i - 1
            exception[i, 7] = i - 100

        res2 = self._stat_past_with_period(arr=inputs1, period=100)
        res3 = stat_past_with_period_py(arr=inputs1, period=100)
        self._ndarray_almost_equals(res1, exception)
        self._ndarray_almost_equals(res2, exception)
        self._ndarray_almost_equals(res3, exception)

    @classmethod
    def _ndarray_almost_equals(cls, arr1: ndarray, arr2: ndarray):
        columns = ["1%", "5%", "10%", "90%", "95%", "99%", "highest", "lowest"]
        df1 = DataFrame(data=arr1, columns=columns)
        df2 = DataFrame(data=arr2, columns=columns)
        assert cls.dataframe_almost_equals(df1, df2, on_index=True)

    @classmethod
    def _stat_past_with_period(cls, arr: ndarray, period: int) -> ndarray:
        CLOSEd, HIGHd, LOWd = 0, 1, 2

        num = len(arr)
        res = np.zeros([num, 8], dtype=float)

        quans = [1, 5, 10, 90, 95, 99]
        for i in range(period, num):
            # 计算
            data = arr[i - period : i, CLOSEd]
            res[i, 0:6] = [
                np.percentile(data, q) for q in quans
            ]  # 1%, 5%, 10%, 90%, 95%, 99%
            res[i, 6] = np.max(arr[i - period : i, HIGHd])  # 最高
            res[i, 7] = np.min(arr[i - period : i, LOWd])  # 最低
        return res
