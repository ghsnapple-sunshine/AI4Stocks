from buffett.adapter.numpy import np, ndarray
from buffett.adapter.pandas import DataFrame
from buffett.adapter.pendulum import DateTime
from buffett.cython.zdf.zdf_stat_ import stat_past_with_period
from test import SimpleTester


class TestStatisticsAnalyst(SimpleTester):
    @classmethod
    def _setup_once(cls):
        cls._inputs = np.random.randint(0, 100, size=(5_000, 3))

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
        res1 = stat_past_with_period(arr=self._inputs, period=period)
        dt2 = DateTime.now()
        res2 = stat_past_with_period(arr=self._inputs, period=period)
        dt3 = DateTime.now()
        print(f"calc_with_tree:   {(dt2 - dt1).total_seconds()}")
        print(f"calc_with_numpy:  {(dt3 - dt2).total_seconds()}")
        columns = ["1%", "5%", "10%", "90%", "95%", "99%", "lowest", "highest"]
        df1 = DataFrame(data=res1, columns=columns)
        df2 = DataFrame(data=res2, columns=columns)
        assert self.dataframe_almost_equals(df1, df2, on_index=True)

    @classmethod
    def _stat_past_with_period(cls, arr: ndarray, period: int) -> ndarray:
        CLOSEd, HIGHd, LOWd = 0, 1, 2

        num = len(arr)
        res = np.zeros([num, 8], dtype=float)

        quans = [1, 5, 10, 90, 95, 99]
        for i in range(period, num):
            # 计算
            data = arr[i - 5 : i, CLOSEd]
            res[i, 0:6] = [
                np.percentile(data, q) for q in quans
            ]  # 1%, 5%, 10%, 90%, 95%, 99%
            res[i, 6] = np.max(arr[i - 5 : i, HIGHd])  # 最高
            res[i, 7] = np.min(arr[i - 5 : i, LOWd])  # 最低
        return res
