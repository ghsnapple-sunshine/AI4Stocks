from enum import Enum


class StockCodeType(Enum):
    CODE6 = 1
    CODE8 = 2
    CODE9 = 3


class StockCode:
    def __init__(self, code: str):
        self.code = code

    def Format(self, typ=StockCodeType.CODE6) -> str:
        if typ == StockCodeType.CODE6:
            return self.toCode6()
        elif typ == StockCodeType.CODE8:
            return self.toCode8()
        else:
            return self.toCode9()

    def toCode6(self) -> str:
        return self.code

    def toCode9(self) -> str:
        if self.code[0] == "6":
            return "sh." + self.code
        elif self.code[0] == "0":
            return "sz." + self.code
        elif self.code[0] == "3":
            return "sz." + self.code
        return 'unknown'

    def toCode8(self) -> str:
        if self.code[0] == "6":
            return "sh" + self.code
        elif self.code[0] == "0":
            return "sz" + self.code
        elif self.code[0] == "3":
            return "sz" + self.code
        return 'unknown'

    def __eq__(self, other):
        if isinstance(other, StockCode):
            return self.code == other.code
        elif isinstance(other, str):
            return self.code == other
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return self.code
