from pandas import DataFrame

from ai4stocks.common.constants import META_COLS, STOCK_LIST_TABLE
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq
from ai4stocks.download.connect.mysql_operator import MysqlOperator


def create_stock_list_1(op: MysqlOperator) -> DataFrame:
    op.drop_table(STOCK_LIST_TABLE)
    cols = [
        ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
        ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]
    ]
    table_meta = DataFrame(data=cols, columns=META_COLS)
    op.create_table(STOCK_LIST_TABLE, table_meta)
    data = [['000001', '平安银行']]
    df = DataFrame(data=data, columns=['code', 'name'])
    op.insert_data(STOCK_LIST_TABLE, df)
    return df


def create_stock_list_2(op: MysqlOperator) -> DataFrame:
    op.drop_table(STOCK_LIST_TABLE)
    cols = [
        ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
        ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]
    ]
    table_meta = DataFrame(data=cols, columns=META_COLS)
    op.create_table(STOCK_LIST_TABLE, table_meta)
    data = [['000001', '平安银行'],
            ['600000', '浦发银行']]
    df = DataFrame(data=data, columns=['code', 'name'])
    op.insert_data(STOCK_LIST_TABLE, df)
    return df


def create_stock_list_ex(op: MysqlOperator) -> DataFrame:
    op.drop_table(STOCK_LIST_TABLE)
    cols = [
        ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.KEY],
        ['name', MysqlColType.STOCK_NAME, MysqlColAddReq.NONE]
    ]
    table_meta = DataFrame(data=cols, columns=META_COLS)
    op.create_table(STOCK_LIST_TABLE, table_meta)
    data = [['000795', '英洛华']]
    df = DataFrame(data=data, columns=['code', 'name'])
    op.insert_data(STOCK_LIST_TABLE, df)
    return df
