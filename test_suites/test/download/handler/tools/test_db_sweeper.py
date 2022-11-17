from pandas import DataFrame

from buffett.common.constants.col.meta import META_COLS
from buffett.common.constants.col.stock import CODE, NAME
from buffett.common.constants.table import STK_LS
from buffett.download.mysql.types import ColType, AddReqType
from test import Tester, DbSweeper


class TestDbSweeper(Tester):
    def test_sweep(self):
        self._create_table()
        DbSweeper.cleanup()
        data = self.operator.select_data(STK_LS)
        assert data.empty

    def _create_table(self):
        data = [
            [CODE, ColType.CODE, AddReqType.KEY],
            [NAME, ColType.CODE, AddReqType.NONE],
        ]
        df = DataFrame(data=data, columns=META_COLS)
        self.operator.create_table(STK_LS, df, True)
