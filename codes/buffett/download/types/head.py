from __future__ import annotations

from buffett.common import ComparableEnum
from buffett.common.constants.col import DATETIME, DATE, OPEN, CLOSE, HIGH, LOW, CJL


class HeadType(ComparableEnum):
    """
    列头的类型
    """

    DATETIME = 1
    DATE = 2
    OPEN = 10
    CLOSE = 11
    HIGH = 12
    LOW = 13
    CJL = 20

    def __str__(self):
        HEAD_TYPE_DICT = {
            HeadType.DATETIME: DATETIME,
            HeadType.DATE: DATE,
            HeadType.OPEN: OPEN,
            HeadType.CLOSE: CLOSE,
            HeadType.HIGH: HIGH,
            HeadType.LOW: LOW,
            HeadType.CJL: CJL,
        }
        return HEAD_TYPE_DICT[self]
