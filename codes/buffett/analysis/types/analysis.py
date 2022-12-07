from buffett.common import ComparableEnum
from buffett.common.magic import empty_method


class AnalystType(ComparableEnum):
    _DICT = None

    PATTERN = 1
    STAT = 2

    def __new__(cls, *args, **kwargs):
        cls._initialize()
        return super(AnalystType, cls).__new__(cls, *args, **kwargs)

    @classmethod
    def _initialize(cls):
        cls._DICT = {
            cls.PATTERN: "Pattern",
            cls.STAT: "Stat",
        }
        cls._initialize = empty_method

    def __str__(self):
        return self._DICT[self]
