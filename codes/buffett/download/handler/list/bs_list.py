from typing import Optional

from buffett.adapter.baostock import bs
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import STK_META
from buffett.common.constants.table import STK_LS
from buffett.download.handler.base import FastHandler
from buffett.download.mysql import Operator

CODE_NAME = "code_name"
TYPE = "type"


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
            {CODE: stocks[CODE].apply(lambda x: x[-6:]), NAME: stocks[CODE_NAME]}
        )
        # 剔除三板市场的股票
        stocks = stocks[stocks[CODE].apply(lambda x: (x[0] != "4") & (x[0] != "8"))]
        return stocks

    def _save_to_database(self, df: DataFrame) -> None:
        self._operator.create_table(name=STK_LS, meta=STK_META)
        self._operator.try_insert_data(name=STK_LS, df=df, update=True, meta=STK_META)

    def select_data(self) -> Optional[DataFrame]:
        """
        获取股票清单

        :return:
        """
        return self._operator.select_data(STK_LS)
