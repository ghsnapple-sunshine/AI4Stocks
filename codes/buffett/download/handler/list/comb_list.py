from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.common.constants.table import STK_LS
from buffett.download.handler import Handler
from buffett.download.handler.list.bs_list import BsStockListHandler
from buffett.download.handler.list.sse_list import SseStockListHandler
from buffett.download.mysql import Operator


class CombineStockListHandler(Handler):
    def __init__(self, operator: Operator):
        super(CombineStockListHandler, self).__init__(operator=operator)
        self._sse_handler = SseStockListHandler(operator=operator)
        self._bs_handler = BsStockListHandler(operator=operator)

    def obtain_data(self) -> None:
        """
        通过SSE Handler和Bs Handler获取数据

        :return:
        """
        self._sse_handler.obtain_data()
        self._bs_handler.obtain_data()

    def select_data(self) -> Optional[DataFrame]:
        """
        获取股票清单

        :return:
        """
        return self._operator.select_data(STK_LS)
