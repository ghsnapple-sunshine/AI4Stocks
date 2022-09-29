from pandas import DataFrame

from ai4stocks.common.constants import STOCK_LIST_TABLE, META_COLS
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq
from test.common.base_test import BaseTest
from test.common.db_sweeper import DbSweeper


class TestDbSweeper(BaseTest):
    def test_sweep(self):
        self.__create_table__()
        DbSweeper.clean_up()
        data = self.op.get_table(STOCK_LIST_TABLE)
        assert data.empty

    def __create_table__(self):
        data = [['code', MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
                ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]]
        df = DataFrame(data=data, columns=META_COLS)
        self.op.create_table(STOCK_LIST_TABLE, df, True)
