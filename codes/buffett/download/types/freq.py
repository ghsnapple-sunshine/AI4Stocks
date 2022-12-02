from buffett.adapter.pendulum import Duration
from buffett.common import ComparableEnum


class FreqType(ComparableEnum):
    DAY = 1
    MIN5 = 2

    def ak_format(self) -> str:
        RECORD_TYPE_DICT = {FreqType.DAY: "daily", FreqType.MIN5: "5"}
        return RECORD_TYPE_DICT[self]

    def to_duration(self) -> Duration:
        RECORD_TYPE_DICT = {
            FreqType.DAY: Duration(days=1),
            FreqType.MIN5: Duration(minutes=5),
        }
        return RECORD_TYPE_DICT[self]

    def __str__(self):
        RECORD_TYPE_DICT = {FreqType.DAY: "day", FreqType.MIN5: "min5"}
        return RECORD_TYPE_DICT[self]
