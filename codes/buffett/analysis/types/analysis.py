from buffett.common import ComparableEnum
from buffett.common.magic import empty_method


class AnalystType(ComparableEnum):
    PATTERN = 1
    STAT = 2

    @classmethod
    def _initialize(cls):
        cls._DICT = {
            cls.PATTERN: "Pattern",
            cls.STAT: "Stat",
        }
        cls._initialize = empty_method

    def __str__(self):
        self._initialize()
        return self._DICT[self]
