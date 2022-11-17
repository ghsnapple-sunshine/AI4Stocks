from buffett.common import ComparableEnum


class AggType(ComparableEnum):
    NONE = 0
    COUNT = 1
    SUM = 2

    def sql_format(self) -> str:
        SQL_FORMAT_DICT = {AggType.NONE: "", AggType.COUNT: "count", AggType.SUM: "sum"}
        return SQL_FORMAT_DICT[self]
