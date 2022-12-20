from pandas import DataFrame

from buffett.common.constants.col.meta import META_COLS
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import STK_META
from buffett.common.constants.table import STK_LS
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql.types import ColType, AddReqType
from test import Tester, DbSweeper


class TestDbSweeper(Tester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_sweep(self):
        self._create_table()
        DbSweeper.cleanup()
        data = self._operator.select_data(name=STK_LS, meta=STK_META)
        assert dataframe_not_valid(data)

    def _create_table(self):
        data = [
            [CODE, ColType.CODE, AddReqType.KEY],
            [NAME, ColType.CODE, AddReqType.NONE],
        ]
        df = DataFrame(data=data, columns=META_COLS)
        self._operator.create_table(STK_LS, df, True)
