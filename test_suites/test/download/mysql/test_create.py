import numpy as np

from buffett.common import create_meta
from buffett.download.mysql.types import ColType, AddReqType
from test import Tester, DbSweeper


class TestCreate(Tester):
    def setUp(self) -> None:
        super(TestCreate, self).setUp()
        self._META = create_meta(meta_list=[['A', ColType.INT32, AddReqType.KEY],
                                            ['B', ColType.INT32, AddReqType.NONE],
                                            ['C', ColType.INT32, AddReqType.NONE],
                                            ['D', ColType.INT32, AddReqType.NONE]])
        self._META2 = create_meta(meta_list=[['A', ColType.INT32, AddReqType.KEY],
                                             ['B', ColType.INT32, AddReqType.NONE],
                                             ['C', ColType.INT32, AddReqType.NONE],
                                             ['D', ColType.INT32, AddReqType.NONE],
                                             ['E', ColType.INT32, AddReqType.NONE],
                                             ['F', ColType.INT32, AddReqType.NONE],
                                             ['G', ColType.INT32, AddReqType.NONE]])
        self._META3 = create_meta(meta_list=[['A', ColType.INT32, AddReqType.KEY],
                                             ['B', ColType.FLOAT, AddReqType.NONE],
                                             ['C', ColType.FLOAT, AddReqType.NONE],
                                             ['D', ColType.FLOAT, AddReqType.NONE],
                                             ['E', ColType.FLOAT, AddReqType.NONE],
                                             ['F', ColType.FLOAT, AddReqType.NONE],
                                             ['G', ColType.FLOAT, AddReqType.NONE]])
        self._META4 = create_meta(meta_list=[['A', ColType.INT32, AddReqType.KEY],
                                             ['B', ColType.FLOAT, AddReqType.NONE],
                                             ['C', ColType.FLOAT, AddReqType.NONE],
                                             ['D', ColType.FLOAT, AddReqType.NONE]])
        self._META5 = create_meta(meta_list=[['A', ColType.INT32, AddReqType.KEY],
                                             ['E', ColType.FLOAT, AddReqType.NONE],
                                             ['F', ColType.FLOAT, AddReqType.NONE],
                                             ['G', ColType.FLOAT, AddReqType.NONE]])

    def test_create(self):
        DbSweeper.cleanup()
        self.operator.create_table(name=self.table_name, meta=self._META, if_not_exist=False)
        meta = self.operator.get_meta(name=self.table_name)
        assert self.compare_dataframe(self._META, meta)

    def test_add_column_fail(self):
        DbSweeper.cleanup()
        self.operator.create_table(name=self.table_name, meta=self._META, if_not_exist=False)
        self.operator.create_table(name=self.table_name, meta=self._META2, if_not_exist=True)
        meta = self.operator.get_meta(name=self.table_name)
        assert self.compare_dataframe(self._META, meta)

    def test_add_column_success(self):
        DbSweeper.cleanup()
        self.operator.create_table(name=self.table_name, meta=self._META, if_not_exist=False)
        self.operator.create_table(name=self.table_name, meta=self._META2, update=True)
        meta = self.operator.get_meta(name=self.table_name)
        assert self.compare_dataframe(self._META2, meta)

    def test_add_column_success(self):
        DbSweeper.cleanup()
        self.operator.create_table(name=self.table_name, meta=self._META, if_not_exist=False)
        self.operator.create_table(name=self.table_name, meta=self._META2, update=True)
        meta = self.operator.get_meta(name=self.table_name)
        assert self.compare_dataframe(self._META2, meta)

    def test_modify_column(self):
        DbSweeper.cleanup()
        self.operator.create_table(name=self.table_name, meta=self._META2, if_not_exist=False)
        self.operator.create_table(name=self.table_name, meta=self._META3, update=True)
        meta = self.operator.get_meta(name=self.table_name)
        assert self.compare_dataframe(self._META3, meta)

    def test_drop_column(self):
        DbSweeper.cleanup()
        self.operator.create_table(name=self.table_name, meta=self._META2, if_not_exist=False)
        self.operator.create_table(name=self.table_name, meta=self._META, update=True)
        meta = self.operator.get_meta(name=self.table_name)
        assert self.compare_dataframe(self._META, meta)

    def test_add_drop_column(self):
        DbSweeper.cleanup()
        self.operator.create_table(name=self.table_name, meta=self._META, if_not_exist=False)
        self.operator.create_table(name=self.table_name, meta=self._META5, update=True)
        meta = self.operator.get_meta(name=self.table_name)
        assert self.compare_dataframe(self._META5, meta)

    def test_modify_drop_column(self):
        DbSweeper.cleanup()
        self.operator.create_table(name=self.table_name, meta=self._META2, if_not_exist=False)
        self.operator.create_table(name=self.table_name, meta=self._META4, update=True)
        meta = self.operator.get_meta(name=self.table_name)
        assert self.compare_dataframe(self._META4, meta)

    def test_add_modify_column(self):
        DbSweeper.cleanup()
        self.operator.create_table(name=self.table_name, meta=self._META, if_not_exist=False)
        self.operator.create_table(name=self.table_name, meta=self._META3, update=True)
        meta = self.operator.get_meta(name=self.table_name)
        assert self.compare_dataframe(self._META3, meta)