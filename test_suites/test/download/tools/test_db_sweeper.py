from pandas import DataFrame

from ai4stocks.constants.meta import META_COLS
from ai4stocks.constants.table import STK_LS
from ai4stocks.download.mysql.types import ColType, AddReqType
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper


class TestDbSweeper(BaseTest):
    def test_sweep(self):
        self._create_table()
        DbSweeper.cleanup()
        data = self.operator.get_table(STK_LS)
        assert data.empty

    def _create_table(self):
        data = [['code', ColType.STOCK_CODE, AddReqType.KEY],
                ['name', ColType.STOCK_NAME, AddReqType.NONE]]
        df = DataFrame(data=data, columns=META_COLS)
        self.operator.create_table(STK_LS, df, True)
