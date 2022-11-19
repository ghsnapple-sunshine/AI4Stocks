from buffett.adapter.pandas import DataFrame
from buffett.common import Code, create_meta
from buffett.common.constants.col.stock import CODE, NAME
from buffett.common.constants.table import STK_LS
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType


def create_1stock(operator: Operator) -> DataFrame:
    """
    创建只有一支股票的StockList

    :param operator:
    :return:
    """
    operator.drop_table(STK_LS)
    table_meta = create_meta(
        meta_list=[
            [CODE, ColType.CODE, AddReqType.KEY],
            [NAME, ColType.CODE, AddReqType.NONE],
        ]
    )
    operator.create_table(STK_LS, table_meta)
    data = [["000001", "平安银行"]]
    df = DataFrame(data=data, columns=[CODE, NAME])
    operator.insert_data(STK_LS, df)
    return df


def create_2stocks(operator: Operator) -> DataFrame:
    """
    创建有两支股票的StockList

    :param operator:
    :return:
    """
    operator.drop_table(STK_LS)
    table_meta = create_meta(
        meta_list=[
            [CODE, ColType.CODE, AddReqType.KEY],
            [NAME, ColType.CODE, AddReqType.NONE],
        ]
    )
    operator.create_table(STK_LS, table_meta)
    data = [["000001", "平安银行"], ["600000", "浦发银行"]]
    df = DataFrame(data=data, columns=[CODE, NAME])
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
    table_meta = create_meta(
        meta_list=[
            [CODE, ColType.CODE, AddReqType.KEY],
            [NAME, ColType.CODE, AddReqType.NONE],
        ]
    )
    operator.create_table(STK_LS, table_meta)
    data = [[code.to_code6(), ""]]
    df = DataFrame(data=data, columns=[CODE, NAME])
    operator.insert_data(STK_LS, df)
    return df
