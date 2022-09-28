from pandas import DataFrame

from ai4stocks.common.constants import STOCK_LIST_TABLE, META_COLS
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper


class TestDbSweeper(BaseTest):
    def test_sweep(self):
        self.CreateTable()
        DbSweeper.CleanUp()
        data = self.op.GetTable(STOCK_LIST_TABLE)
        assert data.empty

    def CreateTable(self):
        data = [['code', MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
                ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]]
        df = DataFrame(data=data, columns=META_COLS)
        self.op.CreateTable(STOCK_LIST_TABLE, df, True)
