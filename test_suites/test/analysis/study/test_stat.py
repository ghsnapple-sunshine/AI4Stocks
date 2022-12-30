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

    def test_with_exception(self):
        inputs1 = np.zeros(shape=(500, 3), dtype=float)
        inputs1[:, 0] = np.arange(1, 501, dtype=float)
        inputs1[:, 1] = np.arange(1, 501, dtype=float)
        inputs1[:, 2] = np.arange(1, 501, dtype=float)

        res1 = stat_past_with_period(arr=inputs1, period=100)
        res2 = stat_past_with_period_py(arr=inputs1, period=100)

        exception = np.zeros(shape=(500, 8), dtype=float)
        q = [1, 5, 10, 90, 95, 99]
        for i in range(0, 500 - 100):
            exception[i, 0:6] = [(x * 0.99 + i + 2) for x in q]
            exception[i, 6] = i + 101
            exception[i, 7] = i + 2
        exception = (exception / inputs1[:, 0].reshape((500, 1)) - 1) * 100

        self._ndarray_almost_equals(res1, exception)
        self._ndarray_almost_equals(res2, exception)

    @classmethod
    def _ndarray_almost_equals(cls, arr1: ndarray, arr2: ndarray):
        columns = ["1%", "5%", "10%", "90%", "95%", "99%", "highest", "lowest"]
        df1 = DataFrame(data=arr1, columns=columns)
        df2 = DataFrame(data=arr2, columns=columns)
        assert cls.dataframe_almost_equals(df1, df2, on_index=True)


