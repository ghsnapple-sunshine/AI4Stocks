from buffett.adapter.numpy import np
from buffett.adapter.pandas import DataFrame
from buffett.adapter.pendulum import DateTime
from buffett.common import create_meta
from buffett.download.mysql.types import ColType, AddReqType
from test import Tester, DbSweeper


class TestAlterPerf(Tester):
    def _setup_always(self) -> None:
        super()._setup_always()
        self._META = create_meta(
            meta_list=[
                ["A", ColType.INT32, AddReqType.KEY],
                ["B", ColType.INT32, AddReqType.NONE],
                ["C", ColType.INT32, AddReqType.NONE],
                ["D", ColType.INT32, AddReqType.NONE],
            ]
        )
        self._META2 = create_meta(
            meta_list=[
                ["A", ColType.INT32, AddReqType.KEY],
                ["B", ColType.INT32, AddReqType.NONE],
                ["C", ColType.INT32, AddReqType.NONE],
                ["D", ColType.INT32, AddReqType.NONE],
                ["E", ColType.INT32, AddReqType.NONE],
                ["F", ColType.INT32, AddReqType.NONE],
                ["G", ColType.INT32, AddReqType.NONE],
            ]
        )

    def _prepare(self, size: int):
        self.a = np.arange(0, size, dtype=int)
        self.b = self.a + 1
        self.c = self.b + 2
        self.d = self.c + 3
        self.e = self.a + 11
        self.f = self.a + 12
        self.g = self.a + 13

    """
    测试alter表格以及重建的速度差异
    """

    """
    手动开启
    def test_250_000(self):
        self._prepare(250_000)
        self._flow()

    def test_5_000(self):
        self._prepare(5_000)
        self._flow()
    """

    def _flow(self):
        # 插入重建的方式
        create_times = []
        rebuild_times = []
        for x in range(0, 10):
            DbSweeper.cleanup()
            t1 = self._timer(self._create)
            t2 = self._timer(self._rebuild)
            create_times.append(t1)
            rebuild_times.append(t2)
        print("create cost time:")
        print(create_times)
        print(sum(create_times) / 10)
        print("rebuild cost time:")
        print(rebuild_times)
        print(sum(rebuild_times) / 10)
        # 修改表的方式
        create_times = []
        alter_times = []
        for x in range(0, 10):
            DbSweeper.cleanup()
            t1 = self._timer(self._create)
            t2 = self._timer(self._alter)
            create_times.append(t1)
            alter_times.append(t2)
        print("create cost time:")
        print(create_times)
        print(sum(create_times) / 10)
        print("alter cost time:")
        print(alter_times)
        print(sum(alter_times) / 10)

    def _create(self):
        self._operator.create_table(name=self._table_name, meta=self._META)
        df = DataFrame({"A": self.a, "B": self.b, "C": self.c, "D": self.d})
        self._operator.insert_data(name=self._table_name, df=df)

    def _alter(self):
        sql = f"alter table `{self._table_name}` add column `E` int, add column `F` int, add column `G` int"
        self._operator.execute(sql=sql, commit=True)
        df = DataFrame({"A": self.a, "E": self.e, "F": self.f, "G": self.g})
        self._operator.try_insert_data(
            name=self._table_name, df=df, meta=self._META2, update=True
        )

    def _rebuild(self):
        df = self._operator.select_data(name=self._table_name)
        df["E"], df["F"], df["G"] = self.e, self.f, self.g
        tmp_table_name = self._table_name + "_tmp"
        self._operator.create_table(name=tmp_table_name, meta=self._META2)
        self._operator.insert_data(name=tmp_table_name, df=df)
        self._operator.drop_table(name=self._table_name)
        sql = f"alter table {tmp_table_name} rename to {self._table_name}"
        self._operator.execute(sql=sql, commit=True)

    def _timer(self, attr):
        start = DateTime.now()
        attr()
        end = DateTime.now()
        return (end - start).total_seconds()
