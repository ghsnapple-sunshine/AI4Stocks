from pandas import DataFrame

from buffett.constants.col.stock import CODE, NAME
from buffett.constants.meta import META_COLS
from buffett.constants.table import STK_LS
from buffett.download.mysql.types import ColType, AddReqType
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper


class TestDbSweeper(BaseTest):
    def test_sweep(self):
        self._create_table()
        DbSweeper.cleanup()
        data = self.operator.get_table(STK_LS)
        assert data.empty

    def _create_table(self):
        data = [[CODE, ColType.STOCK_CODE, AddReqType.KEY],
                [NAME, ColType.STOCK_NAME, AddReqType.NONE]]
        df = DataFrame(data=data, columns=META_COLS)
        self.operator.create_table(STK_LS, df, True)
