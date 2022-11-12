from pandas import DataFrame

from buffett.common import Code
from buffett.constants.meta import META_COLS
from buffett.constants.table import STK_LS
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType


def create_1stock(operator: Operator) -> DataFrame:
    """
    创建只有一支股票的StockList

    :param operator:
    :return:
    """
    cols = [
        ['code', ColType.STOCK_CODE_NAME, AddReqType.KEY],
        ['name', ColType.STOCK_CODE_NAME, AddReqType.NONE]
    ]
    table_meta = DataFrame(data=cols, columns=META_COLS)
    operator.create_table(STK_LS, table_meta)
    data = [['000001', '平安银行']]
    df = DataFrame(data=data, columns=['code', 'name'])
    operator.insert_data(STK_LS, df)
    return df


def create_2stocks(operator: Operator) -> DataFrame:
    """
    创建有两支股票的StockList

    :param operator:
    :return:
    """
    operator.drop_table(STK_LS)
    cols = [
        ['code', ColType.STOCK_CODE_NAME, AddReqType.KEY],
        ['name', ColType.STOCK_CODE_NAME, AddReqType.NONE]
    ]
    table_meta = DataFrame(data=cols, columns=META_COLS)
    operator.create_table(STK_LS, table_meta)
    data = [['000001', '平安银行'],
            ['600000', '浦发银行']]
    df = DataFrame(data=data, columns=['code', 'name'])
    operator.insert_data(STK_LS, df)
    return df


def create_ex_1stock(operator: Operator, code: Code) -> DataFrame:
    """
    创建只有一支股票的StockList，股票代码需指定

    :param operator:
    :param code:
    :return:
    """
    operator.drop_table(STK_LS)
    cols = [
        ['code', ColType.STOCK_CODE_NAME, AddReqType.KEY],
        ['name', ColType.STOCK_CODE_NAME, AddReqType.NONE]
    ]
    table_meta = DataFrame(data=cols, columns=META_COLS)
    operator.create_table(STK_LS, table_meta)
    data = [[code.to_code6(), '']]
    df = DataFrame(data=data, columns=['code', 'name'])
    operator.insert_data(STK_LS, df)
    return df
