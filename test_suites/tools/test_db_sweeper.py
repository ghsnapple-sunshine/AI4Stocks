from pandas import DataFrame

from ai4stocks.data_connect.mysql_common import MysqlColType, MysqlColAddReq, MysqlConstants
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper
from pymysql.err import ProgrammingError


class TestDbSweeper(BaseTest):
    def test_sweep(self):
        self.CreateTable()
        DbSweeper.CleanUp()
        try:
            self.op.GetTable(MysqlConstants.STOCK_LIST_TABLE)
            assert False
        except ProgrammingError as e:
            assert e.args[0] == 1146


    def CreateTable(self):
        data = [['code', MysqlColType.STOCK_CODE, MysqlColAddReq.PRIMKEY],
                ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]]
        df = DataFrame(data=data, columns=MysqlConstants.META_COLS)
        self.op.CreateTable(MysqlConstants.STOCK_LIST_TABLE, df, False)