from zdf_stat import stat_past_with_period

from buffett.adapter.numpy import np, ndarray
from buffett.adapter.pandas import DataFrame
from buffett.adapter.pendulum import DateTime
from buffett.cython.zdf.origin import stat_past_with_period as stat_past_with_period_py
from test import SimpleTester


class TestStatisticsAnalyst(SimpleTester):
    _inputs1 = None
    _input1 = None

    @classmethod
    def _setup_once(cls):
        cls._inputs = np.random.random(size=(5_000, 3)) * 100

    def _setup_always(self) -> None:
        pass

    def test_period5(self):
        self._atom_test(5)

    def test_period20(self):
        self._atom_test(20)

    def test_period240(self):
        self._atom_test(240)  # 960 = 5  * (240 / 5)

    def test_period960(self):
        self._atom_test(960)  # 960 = 20 * (240 / 5)

    def _atom_test(self, period: int):
        dt1 = DateTime.now()
        # cython-sbt
        res1 = stat_past_with_period(arr=self._inputs, period=period)
        dt2 = DateTime.now()
        # numpy
        res2 = self._stat_past_with_period(arr=self._inputs, period=period)
        dt3 = DateTime.now()
        # python
        res3 = stat_past_with_period_py(arr=self._inputs, period=period)
        dt4 = DateTime.now()
        print("---------------------------------------")
        t1 = (dt2 - dt1).total_seconds()
        t2 = (dt3 - dt2).total_seconds()
        d2 = round(t2 / t1, 1)
        t3 = (dt4 - dt3).total_seconds()
        d3 = round(t3 / t1, 1)
        print(f"cython-tree:   {t1} {d2}*python-numpy, {d3}*python-tree")
        print(f"python-numpy:  {t2}")
        print(f"python-tree:   {t3}")

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
