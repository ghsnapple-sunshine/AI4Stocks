# 下载沪深股票列表
from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common import create_meta
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.table import STK_LS
from buffett.download.handler.base import FastHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType

_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
        [NAME, ColType.STOCK_NAME, AddReqType.NONE],
    ]
)


class StockListHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        stocks = ak.stock_info_a_code_name()
        # 剔除三板市场的股票
        stocks = stocks[stocks[CODE].apply(lambda x: (x[0] != "4") & (x[0] != "8"))]
        return stocks

    def _save_to_database(self, df: DataFrame) -> None:
        self._operator.create_table(name=STK_LS, meta=_META)
        self._operator.try_insert_data(name=STK_LS, df=df, update=True, meta=_META)

    def select_data(self) -> Optional[DataFrame]:
        """
        获取股票清单

        :return:
        """
        return self._operator.select_data(STK_LS)
