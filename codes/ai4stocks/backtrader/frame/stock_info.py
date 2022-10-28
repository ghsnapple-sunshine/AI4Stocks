from ai4stocks.backtrader.frame.column import Column
from ai4stocks.download.types import HeadType


class StockInfo:
    def __init__(self, data: dict[HeadType, Column]):
        self.__data = data

    def __getitem__(self, head: HeadType) -> Column:
        return self.__data[head]
