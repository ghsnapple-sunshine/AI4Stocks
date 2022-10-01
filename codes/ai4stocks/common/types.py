from enum import Enum
from pendulum import Duration


class FuquanType(Enum):
    # baostock adjustFlag中后复权为1，前复权为2，不复权为3,
    # akshare adjust使用字符串
    NONE = 3
    QIANFUQIAN = 2
    HOUFUQIAN = 1

    def to_req(self) -> str:
        FUQIAN_TYPE_DICT = {
            FuquanType.NONE: '',
            FuquanType.QIANFUQIAN: 'qfq',
            FuquanType.HOUFUQIAN: 'hfq'
        }
        return FUQIAN_TYPE_DICT[self]

    def __str__(self):
        FUQIAN_TYPE_DICT = {
            FuquanType.NONE: 'bfq',
            FuquanType.QIANFUQIAN: 'qfq',
            FuquanType.HOUFUQIAN: 'hfq'
        }
        return FUQIAN_TYPE_DICT[self]


class DataFreqType(Enum):
    DAY = 1
    MIN5 = 2

    def to_req(self) -> str:
        RECORD_TYPE_DICT = {
            DataFreqType.DAY: 'day',
            DataFreqType.MIN5: '5'
        }
        return RECORD_TYPE_DICT[self]

    def to_duration(self) -> Duration:
        RECORD_TYPE_DICT = {
            DataFreqType.DAY: Duration(days=1),
            DataFreqType.MIN5: Duration(minutes=5)
        }
        return RECORD_TYPE_DICT[self]

    def __str__(self):
        RECORD_TYPE_DICT = {
            DataFreqType.DAY: 'day',
            DataFreqType.MIN5: 'min5'
        }
        return RECORD_TYPE_DICT[self]


class DataSourceType(Enum):
    BAOSTOCK = 1
    AKSHARE_DONGCAI = 10

    def to_sql(self):
        SOURCE_TYPE_DICT = {
            DataSourceType.BAOSTOCK: 'bs',
            DataSourceType.AKSHARE_DONGCAI: 'dc'
        }
        return SOURCE_TYPE_DICT[self]

    def __str__(self):
        SOURCE_TYPE_DICT = {
            DataSourceType.BAOSTOCK: 'bs',
            DataSourceType.AKSHARE_DONGCAI: 'ak(东财)'
        }
        return SOURCE_TYPE_DICT[self]

