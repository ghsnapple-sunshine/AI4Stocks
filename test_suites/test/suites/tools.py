from typing import Any

from buffett.adapter.pandas import DataFrame
from buffett.common import Code, create_meta
from buffett.common.constants.col.target import (
    CODE,
    NAME,
    CONCEPT_CODE,
    CONCEPT_NAME,
    INDUSTRY_CODE,
    INDUSTRY_NAME,
    INDEX_CODE,
    INDEX_NAME,
)
from buffett.common.constants.table import STK_LS, CNCP_LS, INDUS_LS, INDEX_LS
from buffett.common.target import Target
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType


def create_1stock(operator: Operator) -> DataFrame:
    """
    创建只有一支股票的StockList

    :param operator:
    :return:
    """
    data = [["000001", "平安银行"]]
    return _create_stocks(operator, data)


def create_2stocks(operator: Operator) -> DataFrame:
    """
    创建有两支股票的StockList

    :param operator:
    :return:
    """
    data = [["000001", "平安银行"], ["600000", "浦发银行"]]
    return _create_stocks(operator, data)


def create_ex_1stock(operator: Operator, code: Code) -> DataFrame:
    """
    创建只有一支股票的StockList，股票代码需指定

    :param operator:
    :param code:
    :return:
    """
    data = [[code.to_code6(), ""]]
    return _create_stocks(operator, data)


def _create_stocks(operator: Operator, data: list[list[Any]]):
    """
    创建stockList

    :param operator:
    :param data:
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
    df = DataFrame(data=data, columns=[CODE, NAME])
    operator.insert_data(STK_LS, df)
    return df


def create_1concept(operator: Operator) -> DataFrame:
    """
    创建只有一支概念的ConceptList

    :param operator:
    :return:
    """
    data = [["BK0493", "新能源"]]
    return _create_concepts(operator, data)


def create_2concepts(operator: Operator) -> DataFrame:
    """
    创建有两支概念的ConceptList

    :param operator:
    :return:
    """
    data = [["BK0490", "军工"], ["BK0493", "新能源"]]
    return _create_concepts(operator, data)


def create_ex_1concept(operator: Operator, target: Target) -> DataFrame:
    """
    创建只有一支概念的ConceptList，概念代码需指定

    :param operator:
    :param target:
    :return:
    """
    data = [[target.code, target.name]]
    return _create_stocks(operator, data)


def _create_concepts(operator: Operator, data: list[list[Any]]) -> DataFrame:
    """
    创建ConceptList

    :param operator:
    :param data:
    :return:
    """
    table_meta = create_meta(
        meta_list=[
            [CONCEPT_CODE, ColType.CODE, AddReqType.KEY],
            [CONCEPT_NAME, ColType.CONCEPT_NAME, AddReqType.NONE],
        ]
    )
    operator.create_table(CNCP_LS, table_meta)
    df = DataFrame(data=data, columns=[CONCEPT_CODE, CONCEPT_NAME])
    operator.insert_data(CNCP_LS, df)
    return df


def create_1industry(operator: Operator) -> DataFrame:
    """
    创建只有一个行业的IndustryList

    :param operator:
    :return:
    """
    data = [["BK1029", "汽车整车"]]
    return _create_industries(operator, data)


def create_2industries(operator: Operator) -> DataFrame:
    """
    创建有两个行业的IndustryList

    :param operator:
    :return:
    """
    data = [["BK1029", "汽车整车"], ["BK1031", "光伏设备"]]
    return _create_industries(operator, data)


def create_ex_1industry(operator: Operator, target: Target) -> DataFrame:
    """
    创建只有一个行业的IndustryList，行业代码需指定

    :param operator:
    :param target:
    :return:
    """
    data = [[target.code, target.name]]
    return _create_industries(operator, data)


def _create_industries(operator: Operator, data: list[list[Any]]) -> DataFrame:
    """
    创建IndustryList

    :param operator:
    :param data:
    :return:
    """
    table_meta = create_meta(
        meta_list=[
            [INDUSTRY_CODE, ColType.CODE, AddReqType.KEY],
            [INDUSTRY_NAME, ColType.CONCEPT_NAME, AddReqType.NONE],
        ]
    )
    operator.create_table(INDUS_LS, table_meta)
    df = DataFrame(data=data, columns=[INDUSTRY_CODE, INDUSTRY_NAME])
    operator.insert_data(INDUS_LS, df)
    return df


def create_1index(operator: Operator) -> DataFrame:
    """
    创建只有一个指数的IndexList

    :param operator:
    :return:
    """
    data = [["000001", "上证指数"]]
    return _create_indexs(operator, data)


def create_2indexs(operator: Operator) -> DataFrame:
    """
    创建有两个指数的IndexList

    :param operator:
    :return:
    """
    data = [["000001", "上证指数"], ["399006", "创业板指"]]
    return _create_indexs(operator, data)


def create_ex_1index(operator: Operator, target: Target) -> DataFrame:
    """
    创建只有一个指数的IndexList，指数代码需指定

    :param operator:
    :param target:
    :return:
    """
    data = [[target.code, target.name]]
    return _create_indexs(operator, data)


def _create_indexs(operator: Operator, data: list[list[Any]]) -> DataFrame:
    """
    创建IndustryList

    :param operator:
    :param data:
    :return:
    """
    table_meta = create_meta(
        meta_list=[
            [INDEX_CODE, ColType.CODE, AddReqType.KEY],
            [INDEX_NAME, ColType.INDEX_NAME, AddReqType.NONE],
        ]
    )
    operator.create_table(INDEX_LS, table_meta)
    df = DataFrame(data=data, columns=[INDEX_CODE, INDEX_NAME])
    operator.insert_data(INDEX_LS, df)
    return df
