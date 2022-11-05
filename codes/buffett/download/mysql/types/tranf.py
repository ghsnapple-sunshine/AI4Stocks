from buffett.common import ComparableEnum


class TransformType(ComparableEnum):
    NONE = 0
    COUNT = 1
    SUM = 2

    def sql_format(self) -> str:
        SQL_FORMAT_DICT = {TransformType.NONE: '',
                           TransformType.COUNT: 'count',
                           TransformType.SUM: 'sum'}
        return SQL_FORMAT_DICT[self]
