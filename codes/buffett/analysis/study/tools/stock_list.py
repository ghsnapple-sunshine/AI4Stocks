from typing import Optional

from buffett.adapter.pandas import pd, DataFrame
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.tools import dataframe_is_valid
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.mysql import Operator


def get_stock_list(operator: Operator) -> Optional[DataFrame]:
    """
    获取股票清单

    :param operator:
    :return:
    """
    sse_stock_list = SseStockListHandler(operator=operator).select_data()
    bs_stock_list = BsStockListHandler(operator=operator).select_data()
    sse_valid = dataframe_is_valid(sse_stock_list)
    bs_valid = dataframe_is_valid(bs_stock_list)
    if sse_valid and bs_valid:
        add_stock_list = pd.subtract(bs_stock_list[[CODE]], sse_stock_list[[CODE]])
        add_stock_list = pd.merge(add_stock_list, bs_stock_list, on=[CODE], how="left")[
            [CODE, NAME]
        ]
        stock_list = pd.concat([sse_stock_list, add_stock_list])
    elif sse_valid:
        stock_list = sse_stock_list
    elif bs_valid:
        stock_list = bs_stock_list[[CODE, NAME]]
    else:
        stock_list = None
    return stock_list
