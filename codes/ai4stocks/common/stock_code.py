from enum import Enum


class StockCodeType(Enum):
    CODE6 = 1
    CODE8 = 2
    CODE9 = 3


class StockCode(str):
    """
    def __init__(self, code: str):
        self.code = code
    """

    """
    def Format(self, typ=StockCodeType.CODE6) -> str:
        if typ == StockCodeType.CODE6:
            return self.toCode6()
        elif typ == StockCodeType.CODE8:
            return self.toCode8()
        else:
            return self.toCode9()
    """

    def to_code6(self) -> str:
        return self

    def to_code9(self) -> str:
        if self[0] == "6":
            return "sh." + self
        elif self[0] == "0":
            return "sz." + self
        elif self[0] == "3":
            return "sz." + self
        return 'unknown'

    def to_code8(self) -> str:
        if self[0] == "6":
            return "sh" + self
        elif self[0] == "0":
            return "sz" + self
        elif self[0] == "3":
            return "sz" + self
        return 'unknown'
