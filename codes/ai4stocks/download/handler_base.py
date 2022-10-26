from ai4stocks.common import StockCode as Code, FuquanType as FType, ColType as CType
from ai4stocks.common.pendelum import DateSpan as Span
from ai4stocks.download.connect import MysqlOperator as Operator


class HandlerParameter:
    def __init__(
            self,
            datespan: Span = None,
            code: Code = None,
            fuquan: FType = None,
            cols: list[CType] = None
    ):
        self.datespan = datespan
        self.code = code
        self.fuquan = fuquan
        self.cols = cols


class HandlerBase:
    def __init__(self, operator: Operator):
        self.operator = operator

    def obtain(self, para: HandlerParameter):
        pass

    def search(self, para: HandlerParameter):
        pass
