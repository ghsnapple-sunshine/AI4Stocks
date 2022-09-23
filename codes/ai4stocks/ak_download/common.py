from enum import Enum


class FuquanType(Enum):
    NONE = 1,
    QIANFUQIAN = 2,
    HOUFUQIAN = 3

    def toString(self):
        FUQIAN_TYPE_DICT = {
            FuquanType.NONE: '',
            FuquanType.QIANFUQIAN: 'qfq',
            FuquanType.HOUFUQIAN: 'hfq'
        }
        return FUQIAN_TYPE_DICT[self]
