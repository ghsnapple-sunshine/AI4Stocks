from __future__ import annotations

from enum import Enum

from buffett.common.pendelum import Duration
from buffett.constants.col import DATETIME, DATE, OPEN, CLOSE, HIGH, LOW, CJL


class HeadType(Enum):
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
            HeadType.CJL: CJL
        }
        return HEAD_TYPE_DICT[self]


class FuquanType(Enum):
    """
    Fuquan类型
    baostock adjustFlag中后复权为1，前复权为2，不复权为3
    akshare adjust使用字符串
    """

    BFQ = 3
    QFQ = 2
    HFQ = 1

    def ak_format(self) -> str:
        FUQUAN_TYPE_DICT = {
            FuquanType.BFQ: '',
            FuquanType.QFQ: 'qfq',
            FuquanType.HFQ: 'hfq'
        }
        return FUQUAN_TYPE_DICT[self]

    def bs_format(self) -> str:
        return str(self.value)

    def __str__(self):
        FUQIAN_TYPE_DICT = {
            FuquanType.BFQ: 'bfq',
            FuquanType.QFQ: 'qfq',
            FuquanType.HFQ: 'hfq'
        }
        return FUQIAN_TYPE_DICT[self]


class FreqType(Enum):
    DAY = 1
    MIN5 = 2

    """
    def to_req(self) -> str:
        RECORD_TYPE_DICT = {
            FreqType.DAY: 'day',
            FreqType.MIN5: '5'
        }
        return RECORD_TYPE_DICT[self]
    """

    def to_duration(self) -> Duration:
        RECORD_TYPE_DICT = {
            FreqType.DAY: Duration(days=1),
            FreqType.MIN5: Duration(minutes=5)
        }
        return RECORD_TYPE_DICT[self]

    def __str__(self):
        RECORD_TYPE_DICT = {
            FreqType.DAY: 'day',
            FreqType.MIN5: 'min5'
        }
        return RECORD_TYPE_DICT[self]


class SourceType(Enum):
    BAOSTOCK = 1
    AKSHARE_DONGCAI = 10

    def sql_format(self):
        SOURCE_TYPE_DICT = {
            SourceType.BAOSTOCK: 'bs',
            SourceType.AKSHARE_DONGCAI: 'dc'
        }
        return SOURCE_TYPE_DICT[self]

    def __str__(self):
        SOURCE_TYPE_DICT = {
            SourceType.BAOSTOCK: 'bs',
            SourceType.AKSHARE_DONGCAI: 'ak(东财)'
        }
        return SOURCE_TYPE_DICT[self]


class CombType:
    """
    封装类型，封装了FuquanType, SourceType, FreqType
    """

    def __init__(self,
                 fuquan: FuquanType = None,
                 source: SourceType = None,
                 freq: FreqType = None):
        self._fuquan = fuquan
        self._source = source
        self._freq = freq

    def with_fuquan(self, fuquan: FuquanType) -> CombType:
        """
        设置fuquan并返回自身

        :param fuquan:      复权类型
        :return:            Self
        """
        self._fuquan = fuquan
        return self

    def with_source(self, source: SourceType) -> CombType:
        """
        设置source并返回自身

        :param source:      数据来源
        :return:            Self
        """
        self._source = source
        return self

    def with_freq(self, freq: FreqType) -> CombType:
        """
        设置freq并返回自身

        :param freq:        数据频率
        :return:            Self
        """
        self._freq = freq
        return self

    def clone(self) -> CombType:
        """
        复制自身

        :return:            复制的对象
        """
        return CombType(source=self._source,
                        fuquan=self._fuquan,
                        freq=self._freq)

    @property
    def source(self) -> SourceType:
        return self._source

    @property
    def fuquan(self) -> FuquanType:
        return self._fuquan

    @property
    def freq(self) -> FreqType:
        return self._freq
