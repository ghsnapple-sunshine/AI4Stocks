from pandas import DataFrame

from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq, MysqlConstants
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper
from pymysql.err import ProgrammingError


class TestDbSweeper(BaseTest):
    def test_sweep(self):
        self.CreateTable()
        DbSweeper.CleanUp()
        data = self.op.GetTable(MysqlConstants.STOCK_LIST_TABLE)
        assert data.empty

    def CreateTable(self):
        data = [['code', MysqlColType.STOCK_CODE, MysqlColAddReq.PRIMKEY],
                ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]]
        df = DataFrame(data=data, columns=MysqlConstants.META_COLS)
        self.op.CreateTable(MysqlConstants.STOCK_LIST_TABLE, df, True)
