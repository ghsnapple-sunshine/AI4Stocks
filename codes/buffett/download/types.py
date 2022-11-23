from __future__ import annotations

from typing import Optional

from buffett.common import ComparableEnum
from buffett.common.constants.col import DATETIME, DATE, OPEN, CLOSE, HIGH, LOW, CJL
from buffett.common.pendulum import Duration


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


class FuquanType(ComparableEnum):
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
            FuquanType.BFQ: "",
            FuquanType.QFQ: "qfq",
            FuquanType.HFQ: "hfq",
        }
        return FUQUAN_TYPE_DICT[self]

    def bs_format(self) -> str:
        return str(self.value)

    def __str__(self):
        FUQIAN_TYPE_DICT = {
            FuquanType.BFQ: "bfq",
            FuquanType.QFQ: "qfq",
            FuquanType.HFQ: "hfq",
        }
        return FUQIAN_TYPE_DICT[self]


class FreqType(ComparableEnum):
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
            FreqType.MIN5: Duration(minutes=5),
        }
        return RECORD_TYPE_DICT[self]

    def __str__(self):
        RECORD_TYPE_DICT = {FreqType.DAY: "day", FreqType.MIN5: "min5"}
        return RECORD_TYPE_DICT[self]


class SourceType(ComparableEnum):
    BAOSTOCK = 1
    AKSHARE_DONGCAI = 10
    AKSHARE_DONGCAI_CONCEPT = 11
    AKSHARE_DONGCAI_INDUSTRY = 12
    AKSHARE_LGLG_PEPB = 20
    AKSHARE_TONGHUASHUN = 100

    def sql_format(self):
        SOURCE_TYPE_DICT = {
            SourceType.BAOSTOCK: "bs",
            SourceType.AKSHARE_DONGCAI: "dc",
            SourceType.AKSHARE_DONGCAI_CONCEPT: "dc_cncp",
            SourceType.AKSHARE_DONGCAI_INDUSTRY: "dc_indus",
            SourceType.AKSHARE_LGLG_PEPB: "lg_pepb",
            SourceType.AKSHARE_TONGHUASHUN: "th",
        }
        return SOURCE_TYPE_DICT[self]

    def __str__(self):
        SOURCE_TYPE_DICT = {
            SourceType.BAOSTOCK: "bs",
            SourceType.AKSHARE_DONGCAI: "ak(东财)",
            SourceType.AKSHARE_DONGCAI_CONCEPT: "ak(东财,概念)",
            SourceType.AKSHARE_DONGCAI_INDUSTRY: "ak(东财,行业)",
            SourceType.AKSHARE_LGLG_PEPB: "ak(乐股,PEPB)",
            SourceType.AKSHARE_TONGHUASHUN: "ak(同花顺)",
        }
        return SOURCE_TYPE_DICT[self]


class CombType:
    """
    封装类型，封装了FuquanType, SourceType, FreqType
    """

    def __new__(
        cls,
        fuquan: Optional[FuquanType] = None,
        source: Optional[SourceType] = None,
        freq: Optional[FreqType] = None,
    ):
        if fuquan is None and source is None and freq is None:
            return None
        return super(CombType, cls).__new__(cls)

    def __init__(
        self,
        fuquan: Optional[FuquanType] = None,
        source: Optional[SourceType] = None,
        freq: Optional[FreqType] = None,
    ):
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
        return CombType(source=self._source, fuquan=self._fuquan, freq=self._freq)

    def __eq__(self, other):
        if isinstance(other, CombType):
            return (
                self._freq == other.freq
                and self._source == other.source
                and self._fuquan == other.fuquan
            )
        return False

    def __hash__(self) -> int:
        return hash(self._freq) ^ hash(self._source) ^ hash(self._fuquan)

    @property
    def source(self) -> Optional[SourceType]:
        return self._source

    @property
    def fuquan(self) -> Optional[FuquanType]:
        return self._fuquan

    @property
    def freq(self) -> Optional[FreqType]:
        return self._freq
