from buffett.common import ComparableEnum


class LogType(ComparableEnum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3

    def __str__(self):
        return self.name.lower()
