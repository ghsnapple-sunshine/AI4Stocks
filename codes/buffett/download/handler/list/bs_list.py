from typing import Optional

import pandas as pd

from buffett.adapter.baostock import bs
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import START_DATE, END_DATE
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import BS_STK_META
from buffett.common.constants.table import BS_STK_LS
from buffett.download.handler.base import FastHandler
from buffett.download.mysql import Operator

CODE_NAME = "code_name"
TYPE = "type"
IPO_DATE = "ipoDate"
OUT_DATE = "outDate"


class BsStockListHandler(FastHandler):
    """
    下载沪深股票列表
    包含退市的股票清单
    """

    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        stocks = bs.query_stock_basic()
        # 筛选股票
        stocks = stocks[stocks[TYPE] == "1"]
        # rename
        stocks = DataFrame(
            {
                CODE: stocks[CODE].apply(lambda x: x[-6:]),
                NAME: stocks[CODE_NAME],
                START_DATE: pd.to_datetime(stocks[IPO_DATE]),
                END_DATE: pd.to_datetime(
                    stocks[OUT_DATE].apply(lambda x: None if x == "" else x)
                ),
            }
        )
        # 剔除三板市场的股票
        stocks = stocks[stocks[CODE].apply(lambda x: (x[0] != "4") & (x[0] != "8"))]
        return stocks

    def _save_to_database(self, df: DataFrame) -> None:
        self._operator.create_table(name=BS_STK_LS, meta=BS_STK_META, update=True)
        self._operator.try_insert_data(
            name=BS_STK_LS, df=df, update=True, meta=BS_STK_META
        )

    def select_data(self) -> Optional[DataFrame]:
        """
        获取股票清单

        :return:
        """
        return self._operator.select_data(BS_STK_LS)
