# 下载沪深股票列表
from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import STK_META
from buffett.common.constants.table import LG_STK_LS
from buffett.download.handler.base import FastHandler
from buffett.download.mysql import Operator

STOCK_NAME = "stock_name"


class AkLgStockListHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        stocks = ak.stock_a_lg_indicator(symbol="all")
        # rename
        stocks = DataFrame({CODE: stocks[CODE], NAME: stocks[STOCK_NAME]})
        # 剔除三板市场的股票
        stocks = stocks[stocks[CODE].apply(lambda x: (x[0] != "4") & (x[0] != "8"))]
        return stocks

    def _save_to_database(self, df: DataFrame) -> None:
        self._operator.create_table(name=LG_STK_LS, meta=STK_META)
        self._operator.try_insert_data(
            name=LG_STK_LS, df=df, update=True, meta=STK_META
        )

    def select_data(self) -> Optional[DataFrame]:
        """
        获取乐股乐股的股票清单

        :return:
        """
        return self._operator.select_data(LG_STK_LS)
