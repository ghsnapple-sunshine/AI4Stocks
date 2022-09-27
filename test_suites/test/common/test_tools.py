from pandas import DataFrame

from ai4stocks.common.constants import META_COLS, STOCK_LIST_TABLE
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq
from ai4stocks.download.connect.mysql_operator import MysqlOperator


class TestTools:
    @staticmethod
    def CreateStockList_2(op: MysqlOperator) -> DataFrame:
        op.DropTable(STOCK_LIST_TABLE)
        cols = [
            ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
            ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(data=cols, columns=META_COLS)
        op.CreateTable(STOCK_LIST_TABLE, table_meta)
        data = [['000001', '平安银行'],
                ['600000', '浦发银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        op.InsertData(STOCK_LIST_TABLE, df)
        return df

    @staticmethod
    def CreateStockList_1(op: MysqlOperator) -> DataFrame:
        op.DropTable(STOCK_LIST_TABLE)
        cols = [
            ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
            ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(data=cols, columns=META_COLS)
        op.CreateTable(STOCK_LIST_TABLE, table_meta)
        data = [['000001', '平安银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        op.InsertData(STOCK_LIST_TABLE, df)
        return df
