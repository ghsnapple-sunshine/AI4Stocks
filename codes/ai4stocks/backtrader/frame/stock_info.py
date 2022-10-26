from ai4stocks.backtrader.frame.column import Column
from ai4stocks.common import ColType as CType


class StockInfo:
    def __init__(self, data: dict[CType, Column]):
        self.__data = data

    def __getitem__(self, col_type: CType) -> Column:
        return self.__data[col_type]
