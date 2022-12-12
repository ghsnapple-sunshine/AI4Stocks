from buffett.common import ComparableEnum
from buffett.common.magic import empty_method


class AnalystType(ComparableEnum):
    PATTERN = 1
    STAT_ZDF = 2

    @classmethod
    def _initialize(cls):
        cls._DICT = {
            cls.PATTERN: "Pattern",
            cls.STAT_ZDF: "Stat(ZDF)",
        }
        cls._SQL_DICT = {
            cls.PATTERN: "pattern",
            cls.STAT_ZDF: "zdf",
        }
        cls._initialize = empty_method

    def __str__(self):
        self._initialize()
        return self._DICT[self]

    def sql_format(self):
        self._initialize()
        return self._SQL_DICT[self]
