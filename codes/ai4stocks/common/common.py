from enum import Enum


class FuquanType(Enum):
    # baostock adjustFlag中后复权为1，前复权为2，不复权为3,
    # akshare adjust使用字符串
    NONE = 3
    QIANFUQIAN = 2
    HOUFUQIAN = 1

    def toString(self) -> str:
        FUQIAN_TYPE_DICT = {
            FuquanType.NONE: '',
            FuquanType.QIANFUQIAN: 'qfq',
            FuquanType.HOUFUQIAN: 'hfq'
        }
        return FUQIAN_TYPE_DICT[self]