from pandas import DataFrame

from ai4stocks.download.data_connect.mysql_common import MysqlColType, MysqlColAddReq, MysqlConstants
from ai4stocks.download.data_connect.mysql_operator import MysqlOperator


class TestTools:
    @staticmethod
    def CreateStockListTable(op: MysqlOperator) -> DataFrame:
        cols = [
            ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.PRIMKEY],
            ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(data=cols, columns=MysqlConstants.META_COLS)
        op.CreateTable(MysqlConstants.STOCK_LIST_TABLE, table_meta)
        data = [['000001', '平安银行'],
                ['600000', '浦发银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        op.InsertData(MysqlConstants.STOCK_LIST_TABLE, df)
        return df
