from buffett.common import create_meta
from buffett.download.mysql.types import ColType, AddReqType
from test import Tester, DbSweeper


class TestCreate(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._META = create_meta(
            meta_list=[
                ["A", ColType.INT32, AddReqType.KEY],
                ["B", ColType.INT32, AddReqType.NONE],
                ["C", ColType.INT32, AddReqType.NONE],
                ["D", ColType.INT32, AddReqType.NONE],
            ]
        )
        cls._META2 = create_meta(
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
        cls._META3 = create_meta(
            meta_list=[
                ["A", ColType.INT32, AddReqType.KEY],
                ["B", ColType.FLOAT, AddReqType.NONE],
                ["C", ColType.FLOAT, AddReqType.NONE],
                ["D", ColType.FLOAT, AddReqType.NONE],
                ["E", ColType.FLOAT, AddReqType.NONE],
                ["F", ColType.FLOAT, AddReqType.NONE],
                ["G", ColType.FLOAT, AddReqType.NONE],
            ]
        )
        cls._META4 = create_meta(
            meta_list=[
                ["A", ColType.INT32, AddReqType.KEY],
                ["B", ColType.FLOAT, AddReqType.NONE],
                ["C", ColType.FLOAT, AddReqType.NONE],
                ["D", ColType.FLOAT, AddReqType.NONE],
            ]
        )
        cls._META5 = create_meta(
            meta_list=[
                ["A", ColType.INT32, AddReqType.KEY],
                ["E", ColType.FLOAT, AddReqType.NONE],
                ["F", ColType.FLOAT, AddReqType.NONE],
                ["G", ColType.FLOAT, AddReqType.NONE],
            ]
        )

    def _setup_always(self) -> None:
        DbSweeper.cleanup()

    def test_create(self):
        self._operator.create_table(
            name=self._table_name, meta=self._META, if_not_exist=False
        )
        meta = self._operator.get_meta(name=self._table_name)
        assert self.compare_dataframe(self._META, meta)

    def test_add_column_fail(self):
        self._operator.create_table(
            name=self._table_name, meta=self._META, if_not_exist=False
        )
        self._operator.create_table(
            name=self._table_name, meta=self._META2, if_not_exist=True
        )
        meta = self._operator.get_meta(name=self._table_name)
        assert self.compare_dataframe(self._META, meta)

    def test_add_column_success(self):
        self._operator.create_table(
            name=self._table_name, meta=self._META, if_not_exist=False
        )
        self._operator.create_table(
            name=self._table_name, meta=self._META2, update=True
        )
        meta = self._operator.get_meta(name=self._table_name)
        assert self.compare_dataframe(self._META2, meta)

    def test_modify_column(self):
        self._operator.create_table(
            name=self._table_name, meta=self._META2, if_not_exist=False
        )
        self._operator.create_table(
            name=self._table_name, meta=self._META3, update=True
        )
        meta = self._operator.get_meta(name=self._table_name)
        assert self.compare_dataframe(self._META3, meta)

    def test_drop_column(self):
        self._operator.create_table(
            name=self._table_name, meta=self._META2, if_not_exist=False
        )
        self._operator.create_table(name=self._table_name, meta=self._META, update=True)
        meta = self._operator.get_meta(name=self._table_name)
        assert self.compare_dataframe(self._META, meta)

    def test_add_drop_column(self):
        self._operator.create_table(
            name=self._table_name, meta=self._META, if_not_exist=False
        )
        self._operator.create_table(
            name=self._table_name, meta=self._META5, update=True
        )
        meta = self._operator.get_meta(name=self._table_name)
        assert self.compare_dataframe(self._META5, meta)

    def test_modify_drop_column(self):
        self._operator.create_table(
            name=self._table_name, meta=self._META2, if_not_exist=False
        )
        self._operator.create_table(
            name=self._table_name, meta=self._META4, update=True
        )
        meta = self._operator.get_meta(name=self._table_name)
        assert self.compare_dataframe(self._META4, meta)

    def test_add_modify_column(self):
        self._operator.create_table(
            name=self._table_name, meta=self._META, if_not_exist=False
        )
        self._operator.create_table(
            name=self._table_name, meta=self._META3, update=True
        )
        meta = self._operator.get_meta(name=self._table_name)
        assert self.compare_dataframe(self._META3, meta)
