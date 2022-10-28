from pandas import DataFrame

from ai4stocks.common import Code
from ai4stocks.common.pendelum import Date, DateSpan
from ai4stocks.constants.meta import META_COLS
from ai4stocks.constants.table import STK_LS
from ai4stocks.download import Para
from ai4stocks.download.mysql import ColType, AddReqType, Operator


def create_stock_list_1(operator: Operator) -> DataFrame:
    operator.drop_table(STK_LS)
    cols = [
        ['code', ColType.STOCK_CODE, AddReqType.KEY],
        ['name', ColType.STOCK_NAME, AddReqType.NONE]
    ]
    table_meta = DataFrame(data=cols, columns=META_COLS)
    operator.create_table(STK_LS, table_meta)
    data = [['000001', '平安银行']]
    df = DataFrame(data=data, columns=['code', 'name'])
    operator.insert_data(STK_LS, df)
    return df


def create_stock_list_2(operator: Operator) -> DataFrame:
    operator.drop_table(STK_LS)
    cols = [
        ['code', ColType.STOCK_CODE, AddReqType.KEY],
        ['name', ColType.STOCK_NAME, AddReqType.NONE]
    ]
    table_meta = DataFrame(data=cols, columns=META_COLS)
    operator.create_table(STK_LS, table_meta)
    data = [['000001', '平安银行'],
            ['600000', '浦发银行']]
    df = DataFrame(data=data, columns=['code', 'name'])
    operator.insert_data(STK_LS, df)
    return df


def create_stock_list_ex(operator: Operator, code: Code) -> DataFrame:
    operator.drop_table(STK_LS)
    cols = [
        ['code', ColType.STOCK_CODE, AddReqType.KEY],
        ['name', ColType.STOCK_NAME, AddReqType.NONE]
    ]
    table_meta = DataFrame(data=cols, columns=META_COLS)
    operator.create_table(STK_LS, table_meta)
    data = [[code.to_code6(), '']]
    df = DataFrame(data=data, columns=['code', 'name'])
    operator.insert_data(STK_LS, df)
    return df

